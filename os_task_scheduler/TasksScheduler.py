import logging
import asyncio
import uuid
import json
import datetime
import subprocess
import re
import os
import shutil
import copy
from distutils.version import LooseVersion
import signal
import pprint
import pika
import socket
import random
import string
from pathlib import Path

from os_task_scheduler import __version__

logging.getLogger("pika").setLevel(logging.INFO)

OS_PROJECT = os.getenv("OS_PROJECT_NAME", "None")
if OS_PROJECT == "None":
    print("Missing environment for OS_PROJECT_NAME. Did you forget to source?")
    exit(1)


def random_string(string_length):
    """Generate a random string with the combination of lowercase and uppercase letters """
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(string_length))


# make sure ansible installed
def check_ansible_version():
    p = subprocess.Popen(
        ["ansible", "--version"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    out, err = p.communicate()
    ret = p.returncode
    if ret:
        raise ImportError("Cant locate ansible is it installed?")

    reg_ex = r".*ansible (\d*\.\d*).*"
    re_res = re.match(reg_ex, out.decode())
    if not re_res:
        raise ImportError("Cant determine ansible version.")

    required_version = LooseVersion("2.3")
    installed_version = LooseVersion(re_res.group(1))
    if installed_version < required_version:
        raise ImportError(
            "Detected {} Please upgrade ansible to {} or higer.".format(
                installed_version, required_version
            )
        )

    os.environ[
        "ANSIBLE_SSH_ARGS"
    ] = "-o ServerAliveInterval=60 -o ServerAliveCountMax=600"
    os.environ["ANSIBLE_SSH_RETRIES"] = "15"


# make sure openstack  installed
def check_openstack_version():
    p = subprocess.Popen(
        ["openstack", "--version"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    out, err = p.communicate()
    ret = p.returncode
    if ret:
        raise ImportError("Cant locate openstack is it installed?")

    reg_ex = r".*openstack (\d*\.\d*\.\d*).*"
    all_out = out.decode() + err.decode()
    re_res = re.search(reg_ex, all_out)
    if not re_res:
        raise ImportError("Cant determine openstack version.")

    required_version = LooseVersion("3.4.0")
    installed_version = LooseVersion(re_res.group(1))
    if installed_version < required_version:
        raise ImportError(
            "Detected {} Please upgrade openstack to {} or higer.".format(
                installed_version, required_version
            )
        )


check_ansible_version()
check_openstack_version()


def extract_access_point_from_outputs(output_dict, access_point_ref):
    logger = logging.getLogger(__name__)
    access_point_host = output_dict
    for level in access_point_ref:
        if level not in access_point_host:
            logger.error(
                "Cant find {} in outputs. missing {}".format(access_point_ref, level)
            )
            return None
        else:
            access_point_host = access_point_host[level]
    return access_point_host


def parse_json_output(output):
    """
    Parse command output formatted as json
    """
    return json.loads(output.replace("\\n", ""))


def parse_json_stack_outputs(output):
    raw_outputs = parse_json_output(output)
    ret_outputs = {}
    for output_name, output_str in raw_outputs.items():
        output_struct = eval(output_str)
        ret_outputs[output_name] = output_struct["output_value"]

    return ret_outputs


class TasksScheduler(object):
    """
    managing multiuple stacks and jobs.
    """

    # Stack initial state before stack create call returns
    STACK_STATUS_INIT = "Init"
    # Stack create request was successful
    STACK_STATUS_CREATE_IN_PROGRESS = "CreateInProgress"
    # All resources were successfully allocated by Openstack and
    # VMs are running.
    STACK_STATUS_CREATED = "Created"
    # Ready tasks were successfully run on the stack
    STACK_STATUS_READY = "Ready"
    # Ready stack that is currently busy running a Job
    STACK_STATUS_ALLOCATED = "Allocated"
    # Stack delete is about to be sent
    STACK_STATUS_DELETE_REQUESTED = "DeleteRequest"
    # Stack delete request was successful
    STACK_STATUS_DELETE_IN_PROGRESS = "DeleteInProgress"
    # Stack deletion finished successfully
    STACK_STATUS_DELETED = "Deleted"
    # Stack in error state
    STACK_STATUS_ERROR = "Error"

    # all states
    STACK_STATUS_SET = {
        STACK_STATUS_INIT,
        STACK_STATUS_CREATE_IN_PROGRESS,
        STACK_STATUS_CREATED,
        STACK_STATUS_READY,
        STACK_STATUS_ALLOCATED,
        STACK_STATUS_DELETE_REQUESTED,
        STACK_STATUS_DELETE_IN_PROGRESS,
        STACK_STATUS_DELETED,
        STACK_STATUS_ERROR,
    }

    # states that represent stack that can still run Jobs
    STACK_STATUS_AVILABLE_SET = {
        STACK_STATUS_INIT,
        STACK_STATUS_CREATE_IN_PROGRESS,
        STACK_STATUS_CREATED,
        STACK_STATUS_READY,
        STACK_STATUS_ALLOCATED,
    }

    WAIT_CREATE_AFTER_FAIL = 60 * 5
    MAX_FAILED_STACKS = 10
    CONTROLLERS_COUNT = 3
    DEFAULT_MAX_PENDING_CREATES = 2
    CONSECUTIVE_READY_FAILS = 10
    MAX_GLOBAL_IN_PROGRESS = 6
    RABBITMQ_HOST = "tscheduler-mq"

    # Openstack states that we handle
    map_os_stack_states = {
        "CREATE_COMPLETE": STACK_STATUS_CREATED,
        "CREATE_FAILED": STACK_STATUS_ERROR,
        "CREATE_IN_PROGRESS": STACK_STATUS_CREATE_IN_PROGRESS,
        "DELETE_COMPLETE": STACK_STATUS_DELETED,
        "DELETE_FAILED": STACK_STATUS_ERROR,
        "DELETE_IN_PROGRESS": STACK_STATUS_DELETE_IN_PROGRESS,
    }

    _wait_all_count = 0

    def __init__(
        self,
        template_path,
        access_point_node_ref,
        ready_tasks_file_path=None,
        name_prefix="tscheduler",
        loop=None,
        ssh_user=None,
        ssh_password=None,
        become=False,
        job_done_cb=None,
        stack_reuse=False,
        delete_on_failure=False,
        stack_limit=0,
        stack_create_params=None,
        auto_wait_on_internal_ips=True,
        ssh_timeout_sec=120,
        extra_stack_create_params=None,
        max_idle_hours=6,
        verbose=False,
        rabbit_host=RABBITMQ_HOST,
        debug_stack="",
        initial_max_pending_creates=DEFAULT_MAX_PENDING_CREATES,
    ):
        """
        Init the job manager class enabling scheduling jobs and waiting for
        them to finish.
        :param template_path: Path of heat template defining the stack to be
        created.
        :param access_point_node_ref: The out field in the template
        representing the public ip access point to the stack
        :param ready_tasks_file_path: Ansible tasks to do before stack can be
         ready for running jobs (wait for somthing....)
        :param name_prefix: Name to assign to stacks created. Existing stacks
        with that prefix will be regarded as orphants and deleted.
        :param loop: Use an external loop instead the default event loop.
        :param ssh_user: Ansible ssh user to use. If not givven will use the
        default ansible system settings.
        :param ssh_password: Password to use. If not givven will use the
        default ansible system settings.
        :param become: should user become a sudo for running the jobs.
        :param job_done_cb: callback to call that indicate a job termination.
        :param stack_reuse: Flag that define if stacks are reused after job
        termination.
        :param stack_limit: hard limit for number of avilable stacks.
        :param stack_create_params: dictionary of parameters to pass to
        stack_create (see --parameter flag of openstack
        stack create)
        :param auto_wait_on_internal_ips: should the library wait for SSH and
        cloud finish on outputs ending with
        "internal_ip" (using same ssh cradentials).
        :param ssh_timeout_sec: timeout value to use in ansible wait actions.
        :param extra_stack_create_params: A dictionary representing
        set of parameters to be used when
        creating stacks
        :param verbose: Be extra verbose in debug. Will affects the outputs of
        ansible tasks.
        """

        logger = logging.getLogger(__name__)
        self._sched_magic = random_string(8)
        logger.info(
            "TasksScheduler.__init__ {} host: {} magic: {} pid: {}".format(
                __version__, socket.gethostname(), self._sched_magic, os.getpid()
            )
        )

        if loop:
            self._loop = loop
            self._external_looping = True
            self._can_auto_stop = False
        else:
            self._loop = asyncio.get_event_loop()
            self._external_looping = False
            self._can_auto_stop = True

        self._ssh_user = ssh_user
        self._ssh_password = ssh_password
        self._become = become
        self._name_prefix = name_prefix
        self._template_path = os.path.abspath(template_path)
        self._access_point_node_ref = access_point_node_ref.split(".")
        self._stacks = {}
        self._pending_jobs = []
        self._handling_jobs = []
        self._n_job_id = 0
        self._job_done_count = 0
        self._job_error_count = 0
        self._verbose = verbose

        # Create the rady playbook
        self._job_dir = os.path.join(
            os.path.expanduser("~"), ".tscheduler", name_prefix
        )
        self._init_jobs_dir()
        if ready_tasks_file_path is not None:
            self._user_ready_tasks_file_path = os.path.abspath(ready_tasks_file_path)
        else:
            self._user_ready_tasks_file_path = None
        self._user_job_done_cb = job_done_cb
        self._stack_reuse = stack_reuse

        self._delete_on_failure = delete_on_failure
        self._stack_limit = stack_limit
        self._orig_stack_limit = stack_limit
        self._process_id = {}
        self._process_id_killed = {}
        self._failed_creates = 0
        self._ssh_timeout_sec = ssh_timeout_sec
        self._auto_wait_on_internal_ips = auto_wait_on_internal_ips
        self._stack_create_params = stack_create_params
        self._max_idle_seconds = max_idle_hours * 60 * 60
        try:
            self._rabbit_connection = pika.BlockingConnection(
                pika.ConnectionParameters(rabbit_host)
            )
        except Exception:
            logger.exception(f"Error creating rabbit connection with {rabbit_host}.")
            raise RuntimeError
        self._channel = self._rabbit_connection.channel()

        self._channel.exchange_declare(exchange="StackStateX", exchange_type="fanout")
        self._stack_state_queue = self._channel.queue_declare(
            queue="StackState",
            durable=True,
            arguments={"x-message-ttl": 60 * 60 * 1000},
        )
        self._channel.queue_bind(exchange="StackStateX", queue="StackState")

        self._stack_create_queue = self._channel.queue_declare(
            queue="StackCreate",
            durable=True,
            arguments={"x-message-ttl": 24 * 60 * 60 * 1000},
        )

        self._channel.exchange_declare(exchange="StackCreateX", exchange_type="fanout")
        self._channel.queue_bind(exchange="StackCreateX", queue="StackCreate")

        self._job_sum_queue = self._channel.queue_declare(
            queue="gJobSum", arguments={"x-message-ttl": 30 * 1000}
        )

        self._channel.exchange_declare(exchange="JobSumX", exchange_type="fanout")
        self._channel.queue_bind(exchange="JobSumX", queue="gJobSum")

        self._g_stack_sum_queue = self._channel.queue_declare(
            queue="gStackSum", arguments={"x-message-ttl": 30 * 1000}
        )

        self._channel.exchange_declare(exchange="StackSumX", exchange_type="fanout")
        self._channel.queue_bind(exchange="StackSumX", queue="gStackSum")
        self._stack_sum_queue = self._channel.queue_declare(
            "", exclusive=True, arguments={"x-message-ttl": 30 * 1000}
        )
        self._channel.queue_bind(
            exchange="StackSumX", queue=self._stack_sum_queue.method.queue
        )

        self._channel.exchange_declare(exchange="SchedulerCMDX", exchange_type="fanout")
        self._sched_cmd_queue = self._channel.queue_declare(
            queue="gSchedulerCMD", arguments={"x-max-length": 1}
        )
        self._channel.queue_bind(
            exchange="SchedulerCMDX", queue=self._sched_cmd_queue.method.queue
        )

        self._free_extra_stack_create_params = extra_stack_create_params
        if self._free_extra_stack_create_params is not None:
            if type(self._free_extra_stack_create_params) is list:
                logger.warning(
                    "Passing extra_stack_create_params as a list is DEPRECATED!\nChange to 1 dictionary argument"
                )

                if len(self._free_extra_stack_create_params) != 1:
                    raise RuntimeError(
                        "Passing extra_stack_create_params as a list is DEPRECATED!"
                    )

                self._free_extra_stack_create_params = self._free_extra_stack_create_params[
                    0
                ]

        self._can_query_openstack = asyncio.Event()
        self._can_query_openstack.clear()

        self._duplicate_checked = False
        self._next_call_can_check_duplicates = False
        self._skip_check_duplicates = False
        self._initial_max_pending_creates = initial_max_pending_creates
        self._re_init()

        self._debug_stack = debug_stack
        if type(self._debug_stack) is not str:
            raise RuntimeError("debug_stack must be a string")

        if self._debug_stack:
            self._stacks[self._debug_stack] = {}
            self._set_stack_state(
                self._debug_stack, TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS
            )
            self._stacks[self._debug_stack]["user_rc"] = 0
            self._stacks[self._debug_stack]["failure"] = False

    def _re_init(self):
        if not self._external_looping and self._loop is None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        self._log_status_task = asyncio.ensure_future(
            self._log_status(), loop=self._loop
        )

        self._update_states_task = asyncio.ensure_future(
            self._update_states(), loop=self._loop
        )

        self._cmd_task = asyncio.ensure_future(self._cmds_handle(), loop=self._loop)

        self._stack_count_cache = None
        self._max_pendings_create = self._initial_max_pending_creates
        self._last_failed_create_time = 0
        self._global_stats = self.get_global_scheduler_stats()
        self._init_state = True
        self._terminated_jobs = {}
        self._last_5_min_errors = []
        self.consecutive_ready_fails = 0
        self._last_job_done = self._loop.time()
        self._terminate_loop = asyncio.Event()
        self._terminate_loop.clear()
        self._terminate_reason = "None Given"
        self._can_create = True

    def _dump_dict(self):
        out = {}
        for a, v in self.__dict__.items():
            out[a] = str(v)
        return out

    def add_script_job(
        self,
        node_template_ref,
        script_path,
        fin_tasks_path=None,
        post_tasks_path=None,
        pre_tasks_path=None,
        parameters=None,
    ):
        """
        Add a job to be executed on the given node
        :param node_template_ref: The node output name as specified in the
        stack template.
        :param script_path: Path of the script to run on the node.
        :param fin_tasks_path: Anisble task file contain the operation to
        perform when script is done (successfuly)
        :param post_tasks_path: Anisble task file contain the operation to
        perform after script terminated
        (regardless of rc)
        :param pre_tasks_path: Anisble task file contain the operation to
        perform before scrip starts
        :param parameters: paramters list to pass to script.
        :return: job_id
        """
        logger = logging.getLogger(__name__)
        if not self._init_state:
            self._re_init()

        if fin_tasks_path:
            fin_tasks_path = os.path.abspath(fin_tasks_path)
        if pre_tasks_path:
            pre_tasks_path = os.path.abspath(pre_tasks_path)
        if post_tasks_path:
            post_tasks_path = os.path.abspath(post_tasks_path)

        job = {
            "id": self._n_job_id,
            "node_template_ref": node_template_ref.split("."),
            "script_path": script_path,
            "fin_tasks_path": fin_tasks_path,
            "pre_tasks_path": pre_tasks_path,
            "post_tasks_path": post_tasks_path,
            "parameters": copy.deepcopy(parameters),
            "results": None,
            "local": ("127.0.0.1" == node_template_ref),
        }
        self._pending_jobs.append(job)
        self._n_job_id += 1

        logger.debug("Job added\n{0}".format(job))
        return job["id"]

    def add_cmd_job(
        self,
        node_template_ref,
        cmd,
        fin_tasks_path=None,
        post_tasks_path=None,
        pre_tasks_path=None,
    ):
        """
        Add a job to be executed on the given node
        :param node_template_ref: The node output name as specified in the
        stack template.
        :param cmd: A command line to run
        :param fin_tasks_path: Anisble task file contain the operation to
        perform when command is done (successfuly)
        :param post_tasks_path: Anisble task file contain the operation to
        perform after command terminated
        (regardless of rc)
        :param pre_tasks_path: Anisble task file contain the operation to
        perform before command starts
        :return: job_id
        """
        logger = logging.getLogger(__name__)
        if not self._init_state:
            self._re_init()

        if fin_tasks_path:
            fin_tasks_path = os.path.abspath(fin_tasks_path)
        if pre_tasks_path:
            pre_tasks_path = os.path.abspath(pre_tasks_path)
        if post_tasks_path:
            post_tasks_path = os.path.abspath(post_tasks_path)

        job = {
            "id": self._n_job_id,
            "node_template_ref": node_template_ref.split("."),
            "cmd_line": cmd,
            "fin_tasks_path": fin_tasks_path,
            "pre_tasks_path": pre_tasks_path,
            "post_tasks_path": post_tasks_path,
            "local": ("127.0.0.1" == node_template_ref),
        }
        self._pending_jobs.append(job)
        self._n_job_id += 1

        logger.debug("Job added = {}".format(job))
        return job["id"]

    def wait_all(self):
        if self._external_looping:
            raise RuntimeError(
                "External loop was provided to the library's "
                "constructor. {0} method cannot be called."
            )
        if self._loop.is_running():
            raise RuntimeError(
                "External loop was not provided to the "
                "library's"
                " constructor but thread's loop is running."
                " {0} method cannot be called as it tries to "
                "run the loop by itself."
            )

        """
        Called when caller does not manage an external loop to wait for
        pending task to complete.
        """

        logger = logging.getLogger(__name__)
        TasksScheduler._wait_all_count += 1
        logger.debug(
            "wait_all calls count ({}) >> _stacks = {}".format(
                TasksScheduler._wait_all_count, self._stacks
            )
        )
        logger.debug(pprint.pformat(self.__dict__, indent=2, width=400))

        try:
            self._loop.run_until_complete(self._run_lib())
        except RuntimeError as exc:
            logger.error("Runtime error occured: {0}".format(exc))
        except Exception:
            logger.exception(
                "Unhandled exception was raised. " "Below exception will be printed"
            )
        except KeyboardInterrupt:
            logger.info("Interrupted by keystroke")
        finally:
            tasks = asyncio.Task.all_tasks(loop=self._loop)

            fut = asyncio.gather(*tasks)
            fut.cancel()
            try:
                self._loop.run_until_complete(fut)
                fut.exception()
            except Exception:
                logger.exception("run_until_complete exception was raised")
            finally:
                self._init_state = False
                logger.debug("My loop closing it...")
                self._loop.close()
                self._loop = None

                logger.debug(pprint.pformat(self.__dict__, indent=2, width=400))
                logger.debug(
                    "wait_all ({})<< _stacks = {}".format(
                        TasksScheduler._wait_all_count, self._stacks
                    )
                )

    async def _run_lib(self):
        await self._terminate_loop.wait()

    def get_job_total_count(self):
        return self._n_job_id

    def get_stack(self, job_id):
        return self._terminated_jobs[job_id]

    def get_stack_job(self, stack_name):
        return self._stacks[stack_name]["job"]

    def get_stack_attribute(self, stack_name, attribute_name):
        return self._stacks[stack_name]["outputs"][attribute_name]

    def get_stack_state(self, stack_name):
        """
        Get current stack state
        :param stack_name: The stack unique name
        :return: Current stack state
        """
        return self._stacks[stack_name]["status"]

    def get_stack_count(self, status=None):
        """
        Count stacks in specific state.
        :param status: The state to count. None to get total stack count.
        :return: Number counted.
        """
        if not status:
            return len(self._stacks)

        if self._stack_count_cache:
            return self._stack_count_cache[status]

        self._stack_count_cache = dict.fromkeys(TasksScheduler.STACK_STATUS_SET, 0)
        for stack_name, properties in self._stacks.items():
            self._stack_count_cache[properties["status"]] += 1

        return self._stack_count_cache[status]

    def _set_stack_state(self, stack_name, state):
        logger = logging.getLogger(__name__)
        if "status" not in self._stacks[stack_name]:
            logger.debug("Initial Stack {} state set to {}".format(stack_name, state))
        else:
            logger.debug(
                "Stack {} state change {}->{}".format(
                    stack_name, self._stacks[stack_name]["status"], state
                )
            )
        self._stack_count_cache = None
        self._stacks[stack_name]["status"] = state

    async def _manage_stack_population(self):
        """
        Calculate optimal stack count needed currently to meet the demand
        and tries to get there.
        """
        logger = logging.getLogger(__name__)

        total_stacks_required = self.get_stack_count(
            TasksScheduler.STACK_STATUS_ALLOCATED
        ) + len(self._pending_jobs)
        total_stacks_avilable = self._avil_stack_count()

        pending_cancels = self.get_stack_count(
            TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS
        ) + self.get_stack_count(TasksScheduler.STACK_STATUS_DELETE_REQUESTED)

        logger.debug(
            "Stacks Required={} Avilable={} PendingCancel={}".format(
                total_stacks_required, total_stacks_avilable, pending_cancels
            )
        )

        # diff stacks will be the number of stacks we have left to create
        # in case diff_stacks is negative, that means that we've created more
        # stacks than required so the action will be delete the number of over
        # created stacks
        diff_stacks = total_stacks_required - total_stacks_avilable

        if diff_stacks < 0:
            logger.debug("Deleting {} stacks".format(-diff_stacks))
            await self._delete_stacks(-diff_stacks)

        if self._failed_creates >= self.MAX_FAILED_STACKS:
            return

        time_to_recreate = (
            self._last_failed_create_time
            < self._loop.time() - TasksScheduler.WAIT_CREATE_AFTER_FAIL
        )

        if not time_to_recreate:
            return

        if diff_stacks > 0:
            pending_creates = self.get_stack_count(
                TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS
            ) + self.get_stack_count(TasksScheduler.STACK_STATUS_INIT)

            logger.debug("pending_creates {}".format(pending_creates))

            stacks_to_add = min(
                self._max_pendings_create - pending_creates, diff_stacks
            )

            # make sure we dont cross the limit
            if (
                self._stack_limit
                and self._stack_limit - total_stacks_avilable < stacks_to_add
            ):
                stacks_to_add = self._stack_limit - total_stacks_avilable

            if self._can_create and (stacks_to_add > 0):
                logger.debug(
                    "Requesting {} new stacks (Pending={})".format(
                        stacks_to_add, pending_creates + pending_cancels
                    )
                )

                await self._create_stacks(stacks_to_add)

    def _avil_stack_count(self):
        """
        :return: Total stacks that can run Jobs or are in the making.
        """
        ret = 0
        for s in TasksScheduler.STACK_STATUS_AVILABLE_SET:
            ret += self.get_stack_count(s)
        return ret

    def _log_stack_sum(self, counts, stats):
        logger = logging.getLogger(__name__)

        # just to fill cache
        self.get_stack_count(TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS)

        changed = False
        for k in self._stack_count_cache.keys():
            if k not in counts:
                counts[k] = 0

            if counts[k] == self._stack_count_cache[k]:
                continue

            changed = True
            counts[k] = self._stack_count_cache[k]

        total_count = self.get_stack_count()
        if "Total" not in counts or total_count != counts["Total"]:
            changed = True
            counts["Total"] = total_count

        self._global_stats = self.get_global_scheduler_stats()
        for k in self._global_stats.keys():
            if k not in stats:
                stats[k] = 0

            if stats[k] == self._global_stats[k]:
                continue

            max_value = TasksScheduler.MAX_GLOBAL_IN_PROGRESS
            if k == "InProgressCount":
                if stats[k] < max_value and self._global_stats[k] >= max_value:
                    changed = True
                elif stats[k] >= max_value and self._global_stats[k] < max_value:
                    changed = True

            stats[k] = self._global_stats[k]

        message = {
            "header": {
                "version": __version__,
                "project": OS_PROJECT,
                "prefix": self._name_prefix,
                "pid": str(os.getpid()),
                "magic": self._sched_magic,
                "hostname": socket.gethostname(),
                "timestamp": str(datetime.datetime.utcnow()),
            },
            "body": counts,
        }

        zombie_instance = self._failed_creates >= self.MAX_FAILED_STACKS
        if not zombie_instance:
            try:
                self._publish_to_rabbit("StackSumX", message)
            except Exception:  # connection with MQ is not critical to scheduler op
                logger.exception("_publish_to_rabbit exception")

        return changed

    def _publish_to_rabbit(self, exchange, message):
        if self._debug_stack:
            return

        self._channel.basic_publish(
            exchange=exchange,
            routing_key="",
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def _log_job_sum(self, counts):
        logger = logging.getLogger(__name__)

        _counts = {
            "Total": self._n_job_id,
            "Pending": len(self._pending_jobs),
            "Allocated": self.get_stack_count(TasksScheduler.STACK_STATUS_ALLOCATED),
            "Done": self._job_done_count,
            "Error": self._job_error_count,
        }

        changed = False
        for k in _counts.keys():
            if k not in counts:
                counts[k] = 0

            if counts[k] == _counts[k]:
                continue

            changed = True
            counts[k] = _counts[k]

        message = {
            "header": {
                "version": __version__,
                "project": OS_PROJECT,
                "prefix": self._name_prefix,
                "pid": str(os.getpid()),
                "magic": self._sched_magic,
                "hostname": socket.gethostname(),
                "timestamp": str(datetime.datetime.utcnow()),
            },
            "body": counts,
        }

        zombie_instance = self._failed_creates >= self.MAX_FAILED_STACKS
        if not zombie_instance:
            try:
                self._publish_to_rabbit("JobSumX", message)
            # connection with MQ is not critical to scheduler op
            except Exception:
                logger.exception("")

        return changed

    async def _log_status(self):
        logger = logging.getLogger(__name__)
        scounts = {}
        stats = {}
        jcounts = {}

        while True:
            await asyncio.sleep(10)
            schanged = self._log_stack_sum(scounts, stats)
            jchanged = self._log_job_sum(jcounts)

            if not schanged and not jchanged:
                continue

            logger.info(
                "Stacks - Total:{}; CIP:{}; Created:{}; Ready:{}; "
                "Allocated:{}; DIP:{}; Deleted:{}; Error:{}; "
                "InTransit:{}; gInProgress:{}".format(
                    scounts["Total"],
                    scounts[TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS],
                    scounts[TasksScheduler.STACK_STATUS_CREATED],
                    scounts[TasksScheduler.STACK_STATUS_READY],
                    scounts[TasksScheduler.STACK_STATUS_ALLOCATED],
                    scounts[TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS],
                    scounts[TasksScheduler.STACK_STATUS_DELETED],
                    scounts[TasksScheduler.STACK_STATUS_ERROR],
                    scounts[TasksScheduler.STACK_STATUS_INIT]
                    + scounts[TasksScheduler.STACK_STATUS_DELETE_REQUESTED],
                    self._global_stats["InProgressCount"],
                )
            )

            logger.info(
                "Jobs - Total:{}; Pending:{}; Allocated:{}; Done:{}; "
                "Error:{}".format(
                    jcounts["Total"],
                    jcounts["Pending"],
                    jcounts["Allocated"],
                    jcounts["Done"],
                    jcounts["Error"],
                )
            )

    async def _cmds_handle(self):
        logger = logging.getLogger(__name__)

        while True:
            await asyncio.sleep(10)
            self._can_query_openstack.clear()

            # Should be only one message in the queue
            method_frame, header_frame, body = self._channel.basic_get(
                self._sched_cmd_queue.method.queue
            )

            waitcount = 60
            while method_frame is None:
                if waitcount == 0:
                    logging.info(
                        "Queue cmd is empty for long time. publishing a run command"
                    )
                    message = {"cmd_type": "run", "args": {}}
                    self._channel.basic_publish(
                        exchange="SchedulerCMDX",
                        routing_key="",
                        body=json.dumps(message),
                    )
                else:
                    waitcount -= 1
                    logging.info(
                        "Going to sleep ~ 1 second. Because queue cmd is empty"
                    )
                    await asyncio.sleep(random.uniform(0.0, 2.0))

                method_frame, header_frame, body = self._channel.basic_get(
                    self._sched_cmd_queue.method.queue
                )

            self._channel.basic_nack(delivery_tag=method_frame.delivery_tag)

            try:
                msg = json.loads(body.decode("utf-8"))
            except Exception:
                logger.exception("")
                continue

            # some irrelevant message
            if msg is None or type(msg) is not dict or "cmd_type" not in msg:
                logging.error("Bad message in cmd queue: {0}".format(msg))
                continue

            if msg["cmd_type"] == "terminate":
                self.terminate("A command to terminate was sent.")
                continue

            if not self._debug_stack and not self._duplicate_checked:
                continue

            if msg["cmd_type"] == "stop_create":
                if self._can_create:
                    self._can_create = False
                    logging.info("Stop creating new stacks: {0}".format(msg))
                self._can_query_openstack.set()
                continue

            if msg["cmd_type"] == "run":
                if not self._can_create:
                    self._can_create = True
                    logging.info("Continue creating new stacks")
                self._can_query_openstack.set()
                continue

    def stop(self):
        self._can_auto_stop = True

    def get_global_scheduler_stats(self):
        logger = logging.getLogger(__name__)
        global_stats = {"InProgressCount": 0}

        if self._skip_check_duplicates:
            self._duplicate_checked = True

        dup_check = self._next_call_can_check_duplicates and not self._duplicate_checked
        try:
            stacks_sum = {}
            delivery_tag = False
            # get_single message
            method_frame, header_frame, body = self._channel.basic_get(
                self._stack_sum_queue.method.queue
            )

            mypid = str(os.getpid())
            myhostname = socket.gethostname()

            while method_frame and not self._terminate_loop.is_set():
                delivery_tag = method_frame.delivery_tag
                msg = json.loads(body.decode("utf-8"))
                project = msg["header"]["project"]
                prefix = msg["header"]["prefix"]
                pid = mypid
                magic = self._sched_magic
                hostname = myhostname
                if "magic" in msg["header"]:
                    magic = msg["header"]["magic"]
                if "hostname" in msg["header"]:
                    hostname = msg["header"]["hostname"]
                if "pid" in msg["header"]:
                    pid = msg["header"]["pid"]

                if (
                    prefix == self._name_prefix
                    and project == OS_PROJECT
                    and magic == self._sched_magic
                ):
                    # seen myself
                    self._next_call_can_check_duplicates = True

                if (
                    dup_check
                    and prefix == self._name_prefix
                    and project == OS_PROJECT
                    and (
                        magic != self._sched_magic
                        or pid != mypid
                        or hostname != myhostname
                    )
                ):
                    self.terminate(
                        "Duplicate namespace prefix detected {} hostname: {} pid: {} magic: {}".format(
                            prefix, hostname, pid, magic
                        )
                    )

                if project not in stacks_sum.keys():
                    stacks_sum[project] = {}
                stacks_sum[project][prefix] = msg["body"]

                # get_single message
                method_frame, header_frame, body = self._channel.basic_get(
                    self._stack_sum_queue.method.queue
                )

            if delivery_tag is not False:
                self._channel.basic_nack(delivery_tag=delivery_tag, multiple=True)

            for project, context in stacks_sum.items():
                for prefix, summary in context.items():
                    global_stats["InProgressCount"] += (
                        summary["CreateInProgress"]
                        + summary["DeleteRequest"]
                        + summary["DeleteInProgress"]
                    )

        # connection with MQ is not critical to scheduler op
        except Exception:
            logger.exception("")

        if dup_check:
            self._duplicate_checked = True
        return global_stats

    async def _update_states(self):
        logger = logging.getLogger(__name__)
        work_done = (
            self._can_auto_stop
            and len(self._pending_jobs) == 0
            and len(self._handling_jobs) == 0
            and (self._avil_stack_count() == 0 or self._debug_stack != "")
        )
        error_termination = False
        while not work_done and not error_termination:
            await self._can_query_openstack.wait()

            await asyncio.sleep(5)
            try:
                await self._update_stack_states()
            except Exception:
                logger.exception("Exception in _update_stack_states")
                return

            new_error_list = []
            now = self._loop.time()
            for error_ts in self._last_5_min_errors:
                if error_ts + 5 * 60 > now:
                    new_error_list.append(error_ts)

            self._last_5_min_errors = new_error_list
            if len(self._last_5_min_errors) > 10:
                logger.error(
                    "Too many errors in the last 5 minutes. Stoping " "state updates!."
                )
                error_termination = True

            if self._failed_creates >= self.MAX_FAILED_STACKS:
                logger.error(
                    "Too many stacks failed to create. Stoping " "state updates!."
                )
                error_termination = True

            if self._last_job_done < now - self._max_idle_seconds:
                logger.error(
                    "No job was done in the last {} hours. Stopping state "
                    "updates!.".format(self._max_idle_seconds / (60 * 60))
                )
                error_termination = True

            work_done = (
                self._can_auto_stop
                and len(self._pending_jobs) == 0
                and len(self._handling_jobs) == 0
                and (self._avil_stack_count() == 0 or self._debug_stack != "")
            )

        if not error_termination:
            logger.info(
                "No job pending and all stack terminated. " "Stoping state updates!."
            )

        self.terminate("Done with the main task _update_states.")

    async def _delete_stacks(self, n):
        """
        Delete multiple stacks.
        :param n: Number of stacks to delete
        """
        to_be_deleted = []
        # for stack_name, properties in self._stacks.items():
        #     if 0 == n:
        #         break
        #     if (properties['status'] == TasksScheduler.STACK_STATUS_INIT):
        #         to_be_deleted.append(asyncio.ensure_future(
        #             self._delete_stack(stack_name), loop=self._loop))
        #         n -= 1
        #
        # for stack_name, properties in self._stacks.items():
        #     if 0 == n:
        #         break
        #     if properties['status'] == TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS:
        #         to_be_deleted.append(asyncio.ensure_future(
        #             self._delete_stack(stack_name), loop=self._loop))
        #         n -= 1

        for stack_name, properties in self._stacks.items():
            if 0 == n:
                break
            if properties["status"] == TasksScheduler.STACK_STATUS_CREATED:
                to_be_deleted.append(
                    asyncio.ensure_future(
                        self._delete_stack(stack_name), loop=self._loop
                    )
                )
                n -= 1

        for stack_name, properties in self._stacks.items():
            if 0 == n:
                break

            if properties["status"] == TasksScheduler.STACK_STATUS_READY:
                to_be_deleted.append(
                    asyncio.ensure_future(
                        self._delete_stack(stack_name), loop=self._loop
                    )
                )

                n -= 1

        if to_be_deleted:
            await asyncio.wait(to_be_deleted)

    async def _create_stacks(self, n):
        """
        Create multiple stacks.
        :param n: Number of stacks to create
        """
        for i in range(n):
            stack_name = self._name_prefix + "." + str(uuid.uuid4())
            asyncio.ensure_future(self._create_stack(stack_name), loop=self._loop)

    async def _update_stack_states(self):
        """
        Call openstack stack list triggering the callback and updating
        stack states.
        """
        logger = logging.getLogger(__name__)
        cmd = ["openstack", "stack", "list", "-f", "json", "--noindent"]

        try:
            p, stdout, stderr = await self._run_cmd(cmd)
        except Exception:
            logger.exception("exception caugth while listing stacks")
            return

        return_code = p.returncode
        if return_code:
            logger.error("Error listing stacks")
            logger.error(
                "Listing stacks returned={} stderr={}".format(return_code, stderr)
            )

            return

        resulted_stacks = parse_json_output(stdout)
        stacks_seen = set()
        for stack_out in resulted_stacks:
            stack_name = stack_out["Stack Name"]
            stacks_seen.add(stack_name)

            if self._debug_stack:
                if not stack_name.startswith(self._debug_stack):
                    continue

                self._stacks[stack_name] = self._stacks[self._debug_stack]

            elif not stack_name.startswith(self._name_prefix + "."):
                continue

            # Delete an orphan stack
            if stack_name not in self._stacks:
                logger.debug(
                    "Found orphan stack {} in {} state. Deleting!".format(
                        stack_name, stack_out["Stack Status"]
                    )
                )
                self._stacks[stack_name] = {}
                self._set_stack_state(
                    stack_name, self.map_os_stack_states[stack_out["Stack Status"]]
                )

                await self._delete_stack(stack_name)

            message = {
                "header": {
                    "version": __version__,
                    "project": OS_PROJECT,
                    "prefix": self._name_prefix,
                    "pid": str(os.getpid()),
                    "magic": self._sched_magic,
                    "hostname": socket.gethostname(),
                    "timestamp": str(datetime.datetime.utcnow()),
                    "stack-name": stack_name,
                },
                "body": self._stacks[stack_name],
            }
            try:
                self._publish_to_rabbit("StackStateX", message)
            # connection with MQ is not critical to scheduler op
            except Exception:
                logger.exception("_publish_to_rabbit exception")

            if "out_of_list" in self._stacks[stack_name]:
                continue

            # we dont know how to handle such state
            if not stack_out["Stack Status"] in self.map_os_stack_states:
                logger.warning(
                    "Stack Status {} of stack {} is not beeing handled".format(
                        stack_out["Stack Status"], stack_name
                    )
                )
                continue

            stack_status = self.map_os_stack_states[stack_out["Stack Status"]]
            asyncio.ensure_future(
                self._stack_state_machine(
                    stack_name, self._stacks[stack_name]["status"], stack_status
                ),
                loop=self._loop,
            )

        if self._debug_stack:
            return

        for stack_name, record in self._stacks.items():
            if stack_name in stacks_seen:
                continue

            # Deleted
            if record["status"] == TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS:
                asyncio.ensure_future(
                    self._stack_state_machine(
                        stack_name,
                        record["status"],
                        TasksScheduler.STACK_STATUS_DELETED,
                    ),
                    loop=self._loop,
                )

        await self._manage_stack_population()

    async def _create_stack(self, stack_name):
        """
        Create a stack.
        :param stack_name: stack name to be created
        """
        logger = logging.getLogger(__name__)
        if stack_name in self._stacks:
            logger.error(
                "Stack already {} exist with state {}".format(
                    stack_name, self._stacks[stack_name]["status"]
                )
            )
            return None

        self._stacks[stack_name] = {}
        self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_INIT)
        self._stacks[stack_name]["user_rc"] = 0
        self._stacks[stack_name]["failure"] = False

        cmd = [
            "openstack",
            "stack",
            "create",
            "-f",
            "json",
            "--noindent",
            "-t",
            self._template_path,
            stack_name,
        ]

        if self._free_extra_stack_create_params is not None:
            self._stacks[stack_name][
                "stack_extra_params"
            ] = self._free_extra_stack_create_params
            for (k, v) in self._stacks[stack_name]["stack_extra_params"].items():
                cmd.insert(3, "--parameter")
                cmd.insert(4, "{}={}".format(k, str(v)))

        if self._stack_create_params:
            for (k, v) in self._stack_create_params.items():
                cmd.insert(3, "--parameter")
                cmd.insert(4, "{}={}".format(k, str(v)))

        try:
            p, stdout, stderr = await self._run_cmd(cmd, queue_id=stack_name)
        except Exception:
            logger.exception("exception caugth while creating stack")
            return

        return_code = p.returncode
        # SIGTERM
        if return_code == -99:
            pass
        elif not return_code:
            message = {
                "header": {
                    "version": __version__,
                    "project": OS_PROJECT,
                    "prefix": self._name_prefix,
                    "pid": str(os.getpid()),
                    "magic": self._sched_magic,
                    "hostname": socket.gethostname(),
                    "timestamp": str(datetime.datetime.utcnow()),
                    "stack-name": stack_name,
                },
                "body": self._stacks[stack_name],
            }
            try:
                self._publish_to_rabbit("StackCreateX", message)
            except Exception:  # connection with MQ is not critical to scheduler op
                logger.exception("_publish_to_rabbit exception")
            self._set_stack_state(
                stack_name, TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS
            )
        else:
            self._last_failed_create_time = self._loop.time()
            self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_ERROR)
            logger.error(
                "Create stack {} returned={} stderr={}".format(
                    stack_name, return_code, stderr
                )
            )

            if TasksScheduler._should_terminate_on_create_stack_err(
                return_code, stderr
            ):
                self.terminate(
                    reason="Error while creating stack! Exiting The Error: "
                    "{0}".format(stderr)
                )

    def terminate(self, reason=None):
        logger = logging.getLogger(__name__)
        logger.info("Termination is set reason: {}".format(reason))
        self._terminate_loop.set()
        self._terminate_reason = reason
        self._log_status_task.cancel()
        self._update_states_task.cancel()
        self._cmd_task.cancel()

    @staticmethod
    def _should_terminate_on_create_stack_err(returncode, stderr):
        prefix_blacklist = ("ERROR: Property error",)

        for item in prefix_blacklist:
            if stderr.startswith(item):
                return True

        return False

    async def _show_stack(self, stack_name):
        """
        Show a stack.
        :param stack_name: stack name to be shown
        """
        logger = logging.getLogger(__name__)
        cmd = ["openstack", "stack", "show", "-f", "json", "--noindent", stack_name]
        self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_DELETE_REQUESTED)
        try:
            p, stdout, stderr = await self._run_cmd(cmd, queue_id=stack_name)
        except Exception:
            logger.exception("exception caugth while showing stack")
            return None

        return_code = p.returncode
        if return_code:
            self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_ERROR)
            logger.error(
                "Show stack {} returned={} stderr={}".format(
                    stack_name, return_code, stderr
                )
            )

            return None

        return parse_json_output(stdout)

    async def _remove_stack_ports(self, stack_name):
        """
                Delete all the ports in a stack.
                :param stack_name: stack name that it's ports will be deleted
                """
        logger = logging.getLogger(__name__)

        logger.info("deleting stack {} ports.".format(stack_name))
        cmd = ["openstack", "stack", "resource", "list", stack_name, "-f", "json"]
        try:
            p, stdout, stderr = await self._run_cmd(cmd, queue_id=stack_name)
        except Exception:
            logger.exception("exception caugth while listing stack resources")
            return

        return_code = p.returncode
        if not return_code:
            stack_resources = parse_json_output(stdout)

            for stack_resource_out in stack_resources:
                if (
                    "resource_type" in stack_resource_out
                    and stack_resource_out["resource_type"] == "OS::Neutron::Port"
                ):
                    delete_command = [
                        "openstack",
                        "port",
                        "delete",
                        stack_resource_out["physical_resource_id"],
                    ]
                    try:
                        p_delete, stdout_delete, stderr_delete = await self._run_cmd(
                            delete_command, queue_id=stack_name, ignore_errors=True
                        )
                    except Exception:
                        logger.exception("exception caugth while deleting sack port")
                        return

                    if p_delete.returncode:
                        logger.debug(
                            "Failed in deleting port {} from stack {} with rc: {}".format(
                                stack_resource_out["physical_resource_id"],
                                stack_name,
                                p_delete.returncode,
                            )
                        )
        else:
            logger.error("Cant get resources of stack {}".format(stack_name))

    async def _delete_stack(self, stack_name):
        """
        Delete a stack.
        :param stack_name: stack name to be deleted
        """
        logger = logging.getLogger(__name__)

        if self._debug_stack and stack_name.startswith(self._debug_stack):
            return

        if (
            self._stacks[stack_name]["status"]
            not in TasksScheduler.STACK_STATUS_AVILABLE_SET
            and self._stacks[stack_name]["status"] != TasksScheduler.STACK_STATUS_ERROR
        ):
            logger.error(
                "Cant delete {} when state is {}".format(
                    stack_name, self._stacks[stack_name]["status"]
                )
            )
            return None

        if (
            ("create_state" in self._stacks[stack_name])
            and (not self._stacks[stack_name]["create_state"])
            and (
                self._stacks[stack_name]["status"] == TasksScheduler.STACK_STATUS_ERROR
            )
        ):
            show_dict = await self._show_stack(stack_name)
            if show_dict:
                self.info = "Stack {} failed to create with status reason {}".format(
                    stack_name, show_dict["stack_status_reason"]
                )

                if (
                    "Quota exceeded" not in show_dict["stack_status_reason"]
                    and "OverLimit" not in show_dict["stack_status_reason"]
                    and "No valid host was found"
                    not in show_dict["stack_status_reason"]
                    and "Stack DELETE started" not in show_dict["stack_status_reason"]
                ):
                    self._failed_creates += 1
                    logger.error(
                        "Stack {} error {} - failed {}/{}".format(
                            stack_name,
                            show_dict["stack_status_reason"],
                            self._failed_creates,
                            self.MAX_FAILED_STACKS,
                        )
                    )

        if stack_name in self._stacks and "failure" in self._stacks[stack_name]:
            if not self._delete_on_failure and self._stacks[stack_name]["failure"]:
                logger.info(
                    "Will not delete stack {0} because rc is not "
                    "0".format(stack_name)
                )
                self._stacks[stack_name]["out_of_list"] = True
                self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_ERROR)
                return None

        self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_DELETE_REQUESTED)

        await self._remove_stack_ports(stack_name)

        cmd = ["openstack", "stack", "delete", "-y", stack_name]
        try:
            p, stdout, stderr = await self._run_cmd(cmd, queue_id=stack_name)
        except Exception:
            logger.exception("exception caugth while deleting stack")
            return

        return_code = p.returncode
        if return_code == -99:  # SIGTERM
            pass
        elif not return_code:
            self._set_stack_state(
                stack_name, TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS
            )
        else:
            self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_ERROR)
            logger.error(
                "Delete stack {} returned={} stderr={}".format(
                    stack_name, return_code, stderr
                )
            )

    async def _fetch_stack_outputs(self, stack_name):
        logger = logging.getLogger(__name__)
        cmd = [
            "openstack",
            "stack",
            "output",
            "show",
            "--all",
            "-f",
            "json",
            "--noindent",
            stack_name,
        ]
        try:
            p, stdout, stderr = await self._run_cmd(cmd, queue_id=stack_name)
        except Exception:
            logger.exception("exception caugth while listing stack outputs")
            return

        return_code = p.returncode
        if return_code == -99:  # SIGTERM
            pass
        elif not return_code:
            logger.debug(
                "Fetching stack {} outputs finished successfully".format(stack_name)
            )
            self._stacks[stack_name]["outputs"] = parse_json_stack_outputs(stdout)

            self._stacks[stack_name][
                "ready_file_path"
            ] = self._generate_ready_playbook_path(stack_name)

            self._create_ready_playbook(
                self._stacks[stack_name]["ready_file_path"],
                self._user_ready_tasks_file_path,
                stack_name,
            )

            await self._wait_ready_stack(stack_name)
        else:
            logger.error(
                "Cant get stack {} output returned={} stderr={}".format(
                    stack_name, return_code, stderr
                )
            )

            await self._delete_stack(stack_name)

    async def _run_ansible_playbook(self, stack_name, host, playbook_path):
        """
        Run ansible-playbook on stack access point node.
        :param stack_name: stack name
        """
        logger = logging.getLogger(__name__)
        logger.debug(
            "runing ansible playbook on {} of {} stack ({})".format(
                host, stack_name, playbook_path
            )
        )
        extra_vars = [
            "{}={}".format(k, v)
            for k, v in self._stacks[stack_name]["outputs"].items()
            if isinstance(v, str)
        ]
        if self._ssh_user:
            extra_vars.append("ansible_user={}".format(self._ssh_user))
        if self._ssh_password:
            extra_vars.append("ansible_ssh_pass={}".format(self._ssh_password))

        extra_vars.append("host_key_checking=False")
        extra_vars_str = " ".join(extra_vars)

        ssh_args = [
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "StrictHostKeyChecking=no",
        ]
        ssh_args_str = " ".join(ssh_args)
        cmd = ["ansible-playbook"]
        if self._verbose:
            cmd.append("-vvv")

        cmd.extend(
            [
                "-i",
                host + ",",
                "--timeout",
                "60",
                "--ssh-common-args",
                ssh_args_str,
                "--extra-vars",
                extra_vars_str,
                playbook_path,
            ]
        )

        if self._become:
            cmd.insert(-2, "-b")
        if host == "127.0.0.1":
            cmd.insert(-2, "--connection")
            cmd.insert(-2, "local")

        return await self._run_cmd(cmd, queue_id=stack_name)

    def _set_allocated(self, stack_name, job):
        self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_ALLOCATED)
        self._stacks[stack_name]["job"] = job
        self._stacks[stack_name]["job_path"] = self._generate_job_playbook_path(
            stack_name, job
        )
        self._stacks[stack_name]["post_path"] = (
            self._stacks[stack_name]["job_path"] + ".post"
        )

    async def _set_ready(self, stack_name):
        logger = logging.getLogger(__name__)
        self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_READY)

        if not self._pending_jobs:
            return

        job = self._pending_jobs.pop(0)
        if "job" in self._stacks[stack_name]:
            prevjob = self._stacks[stack_name]["job"]
            self._handling_jobs.remove(prevjob)
            logger.info(
                "Stack {1} finished running job {0}".format(prevjob, stack_name)
            )

        self._handling_jobs.append(job)
        try:
            await self._run_a_job(stack_name, job)
        except Exception:
            logger.exception(
                "We have got an exception while were trying "
                " to run a job: {0}\n".format(job)
            )

            self._pending_jobs.append(job)

        if job == self._stacks[stack_name]["job"]:
            self._handling_jobs.remove(job)
            logger.info(
                "Stack {1} finished running last job {0}".format(job, stack_name)
            )

    async def _wait_ready_stack(self, stack_name):
        logger = logging.getLogger(__name__)
        # with no access point Created == Ready
        if not self._access_point_node_ref:
            await self._set_ready(stack_name)
            return

        access_point_host = extract_access_point_from_outputs(
            self._stacks[stack_name]["outputs"], self._access_point_node_ref
        )

        if access_point_host is None:
            logger.error(
                "Stack {} outputs does not contain the access node {}".format(
                    stack_name, self._access_point_node_ref
                )
            )
            await self._delete_stack(stack_name)

        try:
            p, stdout, stderr = await self._run_ansible_playbook(
                stack_name,
                access_point_host,
                self._stacks[stack_name]["ready_file_path"],
            )
        except Exception:
            logger.exception("exception caugth while waitng for stack ready")
            return

        return_code = p.returncode
        if return_code == -99:  # process SIGTERM
            pass
        elif not return_code:
            logger.debug(
                "Waiting ready on stack {} finished successfully".format(stack_name)
            )
            self.consecutive_ready_fails = 0
            self._stack_limit = self._orig_stack_limit
            if (
                self._stacks[stack_name]["status"]
                == TasksScheduler.STACK_STATUS_CREATED
            ):
                # if not from STACK_STATUS_CREATED we probably decided that
                # we no longer need it (DELETED)
                # await asyncio.sleep(5) # found that even ready stacks
                # need some grace
                # dont ever wait think if we decide to delete it ready
                # automaticlly start running a job on dead stack
                await self._set_ready(stack_name)
        else:
            self.consecutive_ready_fails += 1
            logger.error(
                "Wait for ready on stack {0} returned={1} "
                "consecReadyFail={2} Information below:"
                "\nSTDOUT:\n{3}\nSTDERR:\n{4}\n".format(
                    stack_name,
                    return_code,
                    self.consecutive_ready_fails,
                    stdout,
                    stderr,
                )
            )

            if self.consecutive_ready_fails > self.CONSECUTIVE_READY_FAILS:
                self._stack_limit = 1
            await self._delete_stack(stack_name)

    def _kill_process(self, pid):
        logger = logging.getLogger(__name__)
        logger.debug("killing {}".format(pid))
        os.kill(pid, signal.SIGTERM)

    async def _read_stream(self, stream, stream_id, p_id):
        logger = logging.getLogger(__name__)

        out = bytearray()
        if stream:
            while True:
                line = await stream.readline()
                if not line:
                    logger.debug("{} {}: closed".format(stream_id, p_id))
                    break

                if stream_id == "stdout":
                    logger.debug("stdout {}: {}".format(p_id, line))
                elif stream_id == "stderr":
                    logger.error("stderr {}: {}".format(p_id, line))

                out.extend(line)

        return out

    async def _run_cmd(self, cmd, queue_id=None, ignore_errors=False):
        """
        Corerutine for running a process.
        :param cmd: List of the command and arguments to run.
        :param output_parser: The function to use to parse standard output.
        :return: tuple
            1. exit code of the process
            2. parsed standard output or standard error string in case of
            error. If no parsing was requested and the
            process is successful None is returned.
        """
        logger = logging.getLogger(__name__)

        if queue_id and queue_id in self._process_id:
            # we have a running process on this queue so kill the previous
            # process. Use case: when we have a stack that
            # wait for ready and we deside we dont need it any more we can
            # kill the ready waiting process
            logger.info(
                "Terminating process {} for {}".format(
                    self._process_id[queue_id], queue_id
                )
            )
            self._kill_process(self._process_id[queue_id])
            self._process_id_killed[queue_id] = self._process_id[queue_id]

        logger.debug("Launching: {0}".format(" ".join(cmd)))

        p = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            loop=self._loop,
        )

        logger.debug("Going to read outputs of {0}".format(" ".join(cmd)))
        stdout, stderr = await p.communicate()
        if queue_id:
            self._process_id[queue_id] = p.pid

        stdout = stdout.decode().strip()
        stderr = stderr.decode().strip()

        logger.debug(
            "Waiting process {0} with cmd: {1} complete. Information below:"
            "\nSTDOUT:\n{2}\nSTDERR:\n{3}\n".format(
                p.pid, " ".join(cmd), stdout, stderr
            )
        )

        if queue_id and (self._process_id[queue_id] == p.pid):
            self._process_id.pop(queue_id)

        if (
            queue_id
            and (queue_id in self._process_id_killed)
            and (self._process_id_killed[queue_id] == p.pid)
        ):
            return (
                -99,
                "Process {} killed by newer process for {}".format(p.pid, queue_id),
            )

        return_code = p.returncode
        if return_code and not ignore_errors:
            logger.error(
                "Error Procces {} exited with code {} - {}".format(
                    p.pid, return_code, cmd
                )
            )

            self._last_5_min_errors.append(self._loop.time())

        return p, stdout, stderr

    async def _run_a_job(self, stack_name, job):
        logger = logging.getLogger(__name__)

        if self._stacks[stack_name]["status"] != TasksScheduler.STACK_STATUS_READY:
            logger.error(
                "Stack {} not ready to run any job ({})!".format(
                    stack_name, self._stacks[stack_name]["status"]
                )
            )
            return

        logger.debug("Stack {} started work on {}".format(stack_name, job))
        self._set_allocated(stack_name, job)
        if "script_path" in job:
            self._create_job_playbook(
                self._stacks[stack_name]["job_path"], job, stack_name
            )
        elif "cmd_line" in job:
            self._create_cmd_playbook(
                self._stacks[stack_name]["job_path"], job, stack_name
            )

        if job["local"]:
            access_point_node = "127.0.0.1"
        else:
            access_point_node = extract_access_point_from_outputs(
                self._stacks[stack_name]["outputs"],
                self._stacks[stack_name]["job"]["node_template_ref"],
            )

        try:
            p, stdout, stderr = await self._run_ansible_playbook(
                stack_name, access_point_node, self._stacks[stack_name]["job_path"]
            )
        except Exception:
            logger.exception("exception caugth while running a job")
            return

        job = self._stacks[stack_name]["job"]
        job_id = job["id"]
        job["results"] = {}

        res_file_path_str = "{}.results".format(self._stacks[stack_name]["job_path"])
        res_file_path = Path(res_file_path_str)
        if not res_file_path.is_file():
            self._stacks[stack_name]["failure"] = True
            self._job_error_count += 1
            job["results"]["rc"] = 999
            err_str = (
                "Job {} on stack {} failed to create results file. "
                "Information below: \nSTDOUT:\n{}\nSTDERR:\n{}\n".format(
                    job_id, stack_name, job["results"]["rc"], stdout, stderr
                )
            )
            job["results"]["reason"] = err_str

            logger.error(err_str)
        else:
            with open(str(res_file_path)) as f:
                job["results"] = json.load(f)

        self._stacks[stack_name]["user_rc"] = 0
        return_code = p.returncode
        self._last_job_done = self._loop.time()

        # process SIGTERM
        if return_code == -99:
            pass
        elif not return_code:
            self._job_done_count += 1
            if not job["results"]["rc"]:
                logger.debug(
                    "Running Job {} on stack {} finished successfully".format(
                        job_id, stack_name
                    )
                )
            else:
                self._stacks[stack_name]["failure"] = True
                self._stacks[stack_name]["user_rc"] = job["results"]["rc"]
                self._job_error_count += 1

                logger.error(
                    "User job {} on stack {} failed Information below:"
                    "\n{}\n".format(job_id, stack_name, pprint.pformat(job["results"]))
                )
        else:
            self._stacks[stack_name]["user_rc"] = return_code
            self._stacks[stack_name]["failure"] = True
            self._job_error_count += 1
            logger.error(
                "Process running Job {0} on stack {1} failed."
                " returned={2} Information below:"
                "\nSTDOUT:\n{3}\nSTDERR:\n{4}\n".format(
                    job_id, stack_name, return_code, stdout, stderr
                )
            )

        if job["post_tasks_path"]:
            self._create_post_playbook(self._stacks[stack_name]["post_path"], job)
            try:
                p, stdout, stderr = await self._run_ansible_playbook(
                    stack_name, access_point_node, self._stacks[stack_name]["post_path"]
                )
            except Exception:
                logger.exception("exception caugth while running post job tasks")
                return

            rc = p.returncode
            # process SIGTERM
            if rc == -99:
                pass
            elif rc:
                logger.error(
                    "Job {0} on stack {1} post tasks error {2} "
                    "Information below:\nSTDOUT:\n{3}\nSTDERR:\n{4}\n".format(
                        job_id, stack_name, rc, stdout, stderr
                    )
                )

        self._terminated_jobs[job_id] = stack_name

        if self._user_job_done_cb:
            try:
                self._user_job_done_cb(job_id, return_code)
            except Exception:
                err_str = (
                    "Job {0} on stack {1} Information "
                    "below:\nSTDOUT:\n{2}\nSTDERR:\n{3}\n"
                    "user_job_done_cb has thrown an exception".format(
                        job_id, stack_name, stdout, stderr
                    )
                )
                logger.exception(err_str)

        if not self._stack_reuse:
            await self._delete_stack(stack_name)
        elif (
            self._stacks[stack_name]["status"] == TasksScheduler.STACK_STATUS_ALLOCATED
        ):
            # We have a recursion here. However we open a state machine
            # for each stack
            await self._set_ready(stack_name)

    async def _stack_state_machine(self, stack_name, prev_state, new_state):
        logger = logging.getLogger(__name__)

        if new_state == prev_state:
            return

        states = [
            TasksScheduler.STACK_STATUS_CREATED,
            TasksScheduler.STACK_STATUS_READY,
            TasksScheduler.STACK_STATUS_ALLOCATED,
            TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS,
        ]
        if new_state == TasksScheduler.STACK_STATUS_CREATED and prev_state in states:
            # stacks that os see as create completed can be in any of
            # our active states
            return

        logger.debug(
            'Stack {} OS status "{}" was status "{}"'.format(
                stack_name, new_state, prev_state
            )
        )

        if prev_state == TasksScheduler.STACK_STATUS_DELETE_REQUESTED:
            # we are about to send delete we dont care what openstack see
            return

        if (new_state == TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS) and (
            prev_state == TasksScheduler.STACK_STATUS_INIT
        ):
            # Can occur when periodic state update precede
            # the _stack_creating_cb
            return

        if (prev_state == TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS) and (
            new_state != TasksScheduler.STACK_STATUS_DELETED
        ):
            # when stack create failes we send delete on it current
            # state can still be Error
            # orphant stack can be in any state while we delete it...
            return

        if (new_state == TasksScheduler.STACK_STATUS_ERROR) and (
            prev_state != TasksScheduler.STACK_STATUS_ERROR
        ):
            if prev_state == TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS:
                self._stacks[stack_name]["create_state"] = False
                # We dont need to try again for long time
                # (probably out of resource)
                self._last_failed_create_time = self._loop.time()
                self._max_pendings_create = 1
            else:
                # stack changed to error
                logger.error(
                    "Stack {} changed from {} to {}. Deleting!".format(
                        stack_name, prev_state, new_state
                    )
                )

            self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_ERROR)

            await self._delete_stack(stack_name)

            return

        if (new_state == TasksScheduler.STACK_STATUS_CREATED) and (
            prev_state == TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS
        ):
            # stack created we should wait for it to be ready
            self._stacks[stack_name]["create_state"] = True
            self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_CREATED)
            self._max_pendings_create += 1
            if self._max_pendings_create > self._initial_max_pending_creates:
                self._max_pendings_create = self._initial_max_pending_creates
            if self.MAX_GLOBAL_IN_PROGRESS <= self._global_stats["InProgressCount"]:
                self._max_pendings_create = 1

            await self._fetch_stack_outputs(stack_name)

            return

        if (new_state == TasksScheduler.STACK_STATUS_DELETED) and (
            prev_state == TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS
        ):
            # stack deleted nothing more to do with it
            self._set_stack_state(stack_name, TasksScheduler.STACK_STATUS_DELETED)

            return

        if new_state == TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS:
            logger.warning("Stack {} deleted by force majeure!".format(stack_name))
            self._set_stack_state(
                stack_name, TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS
            )

            return

        # Stack we created, with state that we can handle that has changed???
        # I dont think so!
        logger.critical(
            "Stack {} OS status {} Mng status {}".format(
                stack_name, new_state, prev_state
            )
        )

        raise NotImplementedError(
            "Stack {} OS status {} Mng status {}".format(
                stack_name, new_state, prev_state
            )
        )

    def _generate_job_playbook_path(self, stack_name, job):
        full_path = os.path.join(self._job_dir, stack_name + "." + str(job["id"]))

        return full_path

    def _init_jobs_dir(self):
        logger = logging.getLogger(__name__)
        if os.path.exists(self._job_dir):
            logger.debug("deleting {}".format(self._job_dir))
            shutil.rmtree(self._job_dir)

        logger.debug("creating directory {}".format(self._job_dir))
        os.makedirs(self._job_dir)

    def _generate_ready_playbook_path(self, stack_name):
        full_path = os.path.join(self._job_dir, stack_name + "_ready.yml")
        return full_path

    def _copy_job_script(self, stack_name, job, vars):
        logger = logging.getLogger(__name__)
        full_path = (
            os.path.join(self._job_dir, stack_name + "." + str(job["id"])) + ".script"
        )

        logger.debug("copying script {} to {}...".format(job["script_path"], full_path))

        with open(job["script_path"]) as infile, open(full_path, "w") as outfile:
            for line in infile:
                for src, target in vars.items():
                    if isinstance(target, str):
                        line = line.replace("$" + src, target)
                outfile.write(line)

        logger.debug("done.")
        return full_path

    def _create_job_playbook(self, path, job, stack_name):
        params = ""
        if job["parameters"]:
            params = " ".join(str(e) for e in job["parameters"])

        job_path = self._copy_job_script(
            stack_name, job, self._stacks[stack_name]["outputs"]
        )

        with open(path, "w") as f:
            print("---", file=f)
            print("# auto generated by TasksScheduler for job:", file=f)
            print("# {}".format(job), file=f)
            print("- hosts: 'all'", file=f)
            print("  tasks:", file=f)
            if job["pre_tasks_path"]:
                print("    - include: {}".format(job["pre_tasks_path"]), file=f)
            print("    - name: run script", file=f)
            if job["local"]:
                print(
                    "      local_action: script {} {}".format(job_path, params), file=f
                )
            else:
                print("      script: {} {}".format(job_path, params), file=f)
            print("      register: results", file=f)
            print("      failed_when: not results.changed", file=f)
            print(
                "    - local_action: copy content={{{{ results }}}} dest={}.results".format(
                    path
                ),
                file=f,
            )
            if job["fin_tasks_path"]:
                print("    - include: {}".format(job["fin_tasks_path"]), file=f)
                print("      when: results.rc == 0", file=f)
            print("# auto generate done", file=f)

    def _create_post_playbook(self, path, job):
        with open(path, "w") as f:
            print("---", file=f)
            print("# auto generated by TasksScheduler for job:", file=f)
            print("# {}".format(job), file=f)
            print("- hosts: 'all'", file=f)
            print("  tasks:", file=f)
            print("    - include: {}".format(job["post_tasks_path"]), file=f)
            print("# auto generate done", file=f)

    def _create_cmd_playbook(self, path, job, stack_name):
        with open(path, "w") as f:
            print("---", file=f)
            print("# auto generated by TasksScheduler for job:", file=f)
            print("# {}".format(job), file=f)
            print("- hosts: 'all'", file=f)
            print("  tasks:", file=f)
            if job["pre_tasks_path"]:
                print("    - include: {}".format(job["pre_tasks_path"]), file=f)

            job_cmd_line = job["cmd_line"]
            print('    - name: run cmd "{}"'.format(job_cmd_line), file=f)
            for k, v in self._stacks[stack_name]["outputs"].items():
                if isinstance(v, str):
                    job_cmd_line = job_cmd_line.replace("$" + k, v)
            if job["local"]:
                print("      local_action: shell {0}".format(job_cmd_line), file=f)
            else:
                print("      shell: {0}".format(job_cmd_line), file=f)
            print("      register: results", file=f)
            print("      ignore_errors: yes", file=f)
            print("    - debug: var=results", file=f)
            print(
                "    - local_action: copy content={{{{ results }}}} dest={}."
                "results".format(path),
                file=f,
            )
            if job["fin_tasks_path"]:
                print("    - include: {}".format(job["fin_tasks_path"]), file=f)
                print("      when: results.rc == 0", file=f)
            print("# auto generate done", file=f)

    def _create_ready_playbook(self, path, ready_tasks_path, stack_name):
        with open(path, "w") as f:
            print("---", file=f)
            print("# Ready playbook auto generated by TasksScheduler", file=f)
            print("- hosts: 'all'", file=f)
            print("  gather_facts: no", file=f)
            print("  tasks:", file=f)
            print("    - name: wait for SSH connectivity with access node", file=f)
            print(
                "      local_action: wait_for port=22 host={{ inventory_hostname }} state=started "
                "search_regex=OpenSSH sleep=10 delay=10",
                file=f,
            )
            print("    - name: wait for system to become reachable", file=f)
            print(
                "      wait_for_connection: delay=10 sleep=10 timeout={}".format(
                    self._ssh_timeout_sec
                ),
                file=f,
            )
            print("    - name: gather facts", file=f)
            print("      setup:", file=f)
            print("    - name: wait for cloud-init finish", file=f)
            print(
                "      wait_for: path=/var/lib/cloud/instance/boot-finished state=present sleep=10 "
                "delay=10 timeout={}".format(self._ssh_timeout_sec),
                file=f,
            )
            if self._become and self._auto_wait_on_internal_ips:
                print("    - name: install sshpass", file=f)
                print("      package: name=sshpass state=present", file=f)
                print("      register: package_results", file=f)
                print("      retries: 10", file=f)
                print("      delay: 10", file=f)
                print("      until: package_results is success", file=f)

            if self._auto_wait_on_internal_ips:
                for name, value in self._stacks[stack_name]["outputs"].items():
                    if not isinstance(value, str) or not name.endswith("_internal_ip"):
                        continue

                    print(
                        "    - name: wait for SSH connectivity with {} {}".format(
                            name, value
                        ),
                        file=f,
                    )
                    print(
                        "      wait_for: port=22 host={} state=started search_regex=OpenSSH sleep=10 "
                        "delay=10 timeout={}".format(value, self._ssh_timeout_sec),
                        file=f,
                    )
                    print(
                        "    - name: checking /var/lib/cloud/instance/boot-finished",
                        file=f,
                    )
                    print(
                        "      shell: timeout 30 sshpass -p {} ssh "
                        "-o UserKnownHostsFile=/dev/null "
                        "-o StrictHostKeyChecking=no {}@{} cat "
                        "/var/lib/cloud/instance/boot-finished".format(
                            self._ssh_password, self._ssh_user, value
                        ),
                        file=f,
                    )
                    print("      register: results", file=f)
                    print("      until: results.rc == 0", file=f)
                    print("      delay: 15", file=f)
                    print("      retries: 40", file=f)

            if ready_tasks_path is not None:
                print("    - include: {}".format(ready_tasks_path), file=f)
            print("# auto generate done", file=f)
