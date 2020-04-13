import pytest
import OSTaskScheduler.TasksScheduler as TasksScheduler
import asyncio
import subprocess
import os
import shutil
import logging
import pika
import json
import time
import stat


def get_file_list_recursive(dir_path):
    out = []
    for root, directories, filenames in os.walk(dir_path):
        for directory in directories:
            out += get_file_list_recursive(os.path.join(root, directory))
        for filename in filenames:
            out.append(os.path.join(root, filename))

    return out


MANAGER = None
e2e_job_cb_count = 0
e2e_job_cb_error = 0
visited = False


def job_done_cb(jobid, rc):
    global e2e_job_cb_count, e2e_job_cb_error
    logger = logging.getLogger(__name__)

    logger.debug("job_done_cb job={} ansible_rc={}".format(jobid, rc))
    stack_name = MANAGER.get_stack(jobid)
    job = MANAGER.get_stack_job(stack_name)

    if not rc:
        e2e_job_cb_count += 1
        if job["results"]["rc"]:
            logger.debug("job_done_cb job={} rc={}".format(jobid, job["results"]["rc"]))
            e2e_job_cb_error += 1


@pytest.fixture
def manager_stack_e2e(template_path, ready_task_path, post_task_path, rabbit_service):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        ready_tasks_file_path=ready_task_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="scheduler2.test",
        ssh_user="tstuser",
        ssh_password="tstpass",
        job_done_cb=job_done_cb,
        delete_on_failure=True,
        rabbit_host="127.0.0.1",
    )
    return manager


async def send_terminate(rabbit_channel, sleep=10):
    await asyncio.sleep(sleep)
    message = {"cmd_type": "terminate", "args": {}}
    rabbit_channel.basic_publish(
        exchange="SchedulerCMDX", routing_key="", body=json.dumps(message)
    )


def check_service_exists(service_name):
    logger = logging.getLogger(__name__)
    logger.info("Checking if service {0} is up".format(service_name))
    proc = subprocess.Popen(
        ["docker", "inspect", "-f", "'{{.State.Running}}'", service_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    proc.wait(timeout=10)
    return proc.returncode == 0


def login_to_docker_registry():
    login_cmd = ["docker", "login", "-u", "groot", "-p", "fl0ra", "ilreg.dyn"]
    proc = subprocess.Popen(
        login_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    proc.wait(timeout=10)

    if proc.returncode != 0:
        raise ChildProcessError


def remove_service(service_name):
    # stop container
    cmd = "docker stop {service_name}".format(service_name=service_name)
    p = subprocess.Popen(cmd, shell=True)
    p.wait(30)

    # remove container
    cmd = "docker rm {service_name}".format(service_name=service_name)
    p = subprocess.Popen(cmd, shell=True)
    p.wait(30)


def run_rabbit():
    logger = logging.getLogger(__name__)

    login_to_docker_registry()
    cmd = [
        "docker",
        "run",
        "-d",
        "-p",
        "15672:15672",
        "-p",
        "5672:5672",
        "--mount",
        "type=bind,src=/etc/localtime,dst=/etc/localtime,ro=false",
        "--name",
        "rabbitmq",
        "ilreg.dyn/{0}:{1}".format("rabbitmq", "3.6.6-management"),
    ]
    if check_service_exists("rabbitmq"):
        logger.info(
            "Not going to run rabbitmq because it is already running. "
            "You can run it as:\n{0}\n".format(" ".join("rabbitmq"))
        )

        return

    logger.info("Running rabbit service:\n{0}\n".format(" ".join(cmd)))

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    proc.wait(timeout=30)

    if proc.returncode != 0:
        raise ChildProcessError

    time.sleep(10)

    return proc


@pytest.yield_fixture(scope="class")
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    yield loop

    loop.close()


@pytest.yield_fixture()
def function_event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    return loop


def delete_all_stacks(manager):
    logger = logging.getLogger(__name__)
    logger.info("delete_all_stacks")
    cmd = ["openstack", "stack", "delete", "-y", None]
    for name, v in manager._stacks.items():
        if (
            v["status"] != TasksScheduler.TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS
        ) and (v["status"] != TasksScheduler.TasksScheduler.STACK_STATUS_DELETED):
            cmd[-1] = name
            p = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = p.communicate()
            ret = p.returncode
            if ret:
                logger.error(
                    "error running `%s` exited: %d - %s" % (" ".join(cmd), ret, err)
                )


class PopenMockPass(object):
    def __init__(self, cmdList, stdin, stdout, stderr):
        self.returncode = 0
        self.stdout = b"output"
        self.stderr = b"error"
        if cmdList[0] == "ansible" and cmdList[1] == "--version":
            self.stdout = b"ansible 2.3.0.3\n  config file = /etc/ansible/ansible.cfg\n  configured module search path = Default w/o overrides\n"
        elif cmdList[0] == "openstack" and cmdList[1] == "--version":
            self.stderr = b"openstack 3.5.0\n"
        elif (
            cmdList[0] == "openstack" and cmdList[1] == "stack" and cmdList[2] == "list"
        ):
            self.stdout = b'[{"Updated Time": null, "Stack Status": "CREATE_COMPLETE", "Stack Name": "pytest.stam", "ID": "43c6a803-5b9e-4532-ace0-6b9ee9a990b6", "Creation Time": "2017-04-02T10:55:11Z"}]'

    def communicate(self):
        return (self.stdout, self.stderr)


class PopenMockFailOutput(PopenMockPass):
    def __init__(self, cmdList, stdin, stdout, stderr):
        self.returncode = 0
        self.stdout = b"output"
        self.stderr = b"error"
        if cmdList[0] == "ansible" and cmdList[1] == "--version":
            self.stdout = b"ansible 2.0.0.1\n  config file = /etc/ansible/ansible.cfg\n  configured module search path = Default w/o overrides\n"
        if cmdList[0] == "openstack" and cmdList[1] == "--version":
            self.stderr = b"openstack 3.2.0\n"


class PopenMockFailRC(PopenMockPass):
    def __init__(self, cmdList, stdin, stdout, stderr):
        self.returncode = 0
        self.stdout = b"output"
        self.stderr = b"error"
        if cmdList[0] == "ansible" and cmdList[1] == "--version":
            self.returncode = 100
        if cmdList[0] == "openstack" and cmdList[1] == "--version":
            self.returncode = -100


class TasksSchedulerMoc(TasksScheduler.TasksScheduler):
    def __init__(self):
        super().__init__("nosuchpath", "noref")
        self._stacks = {
            "jobmng.stam": {
                "status": TasksScheduler.TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS
            }
        }


@pytest.fixture
def scheduler_mock_pass(monkeypatch):
    monkeypatch.setattr(subprocess, "Popen", PopenMockPass)
    return TasksSchedulerMoc()


def create_stack(stack_name, template_path):
    logger = logging.getLogger(__name__)
    cmd = [
        "openstack",
        "stack",
        "create",
        "-f",
        "json",
        "--noindent",
        "-t",
        template_path,
        stack_name,
    ]
    p = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = p.communicate()
    ret = p.returncode
    if ret:
        logger.error(
            "Error creating stack {} exited: {} - {}".format(stack_name, ret, err)
        )

        return False

    return True


def delete_stack(name):
    logger = logging.getLogger(__name__)
    logger.info("delete_all_stacks")
    cmd = ["openstack", "stack", "delete", "-y", name]
    p = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = p.communicate()
    ret = p.returncode
    if ret:
        logger.error("error deleting stack {} exited: {} - {}".format(name, ret, err))
        return False

    logger.info("deleted stack {}".format(name))
    return True


@pytest.fixture(scope="session")
def rabbit_service():
    run_rabbit()

    rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    channel = rabbit_connection.channel()
    channel.exchange_declare(exchange="SchedulerCMDX", exchange_type="fanout")
    sched_cmd_queue = channel.queue_declare(
        queue="gSchedulerCMD", arguments={"x-max-length": 1}
    )
    channel.queue_bind(exchange="SchedulerCMDX", queue=sched_cmd_queue.method.queue)

    message = {"cmd_type": "run", "args": {}}
    channel.basic_publish(
        exchange="SchedulerCMDX", routing_key="", body=json.dumps(message)
    )

    channel.close()

    yield
    remove_service("rabbitmq")


@pytest.fixture
def rabbit_channel():
    rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
    channel = rabbit_connection.channel()

    yield channel

    message = {"cmd_type": "run", "args": {}}
    channel.basic_publish(
        exchange="SchedulerCMDX", routing_key="", body=json.dumps(message)
    )

    channel.close()


e2e_folder = os.path.join(os.path.expanduser("~"), "test_end_2_end")
e2e_out_folder = os.path.join(e2e_folder, "outputs")
e2e_err_folder = os.path.join(e2e_folder, "errors")
if not os.path.exists(e2e_folder):
    os.makedirs(e2e_folder)
    os.makedirs(e2e_out_folder)
    os.makedirs(e2e_err_folder)


def create_script(name, cmd):
    full_path = os.path.join(e2e_folder, name)
    with open(full_path, "w") as f:
        print("#!/bin/bash", file=f)
        print(cmd, file=f)

    st = os.stat(full_path)
    os.chmod(full_path, st.st_mode | stat.S_IEXEC)

    return full_path


def test_newer_ansible_version_pass(monkeypatch):
    logger = logging.getLogger(__name__)
    logger.info("Start")
    monkeypatch.setattr(subprocess, "Popen", PopenMockPass)
    TasksScheduler.check_ansible_version()
    logger.info("End")


def test_older_ansible_version_fails(monkeypatch):
    logger = logging.getLogger(__name__)
    logger.info("Start")
    monkeypatch.setattr(subprocess, "Popen", PopenMockFailOutput)
    with pytest.raises(ImportError):
        TasksScheduler.check_ansible_version()
    logger.info("End")


def test_ansible_not_installed_fails(monkeypatch):
    logger = logging.getLogger(__name__)
    logger.info("Start")
    monkeypatch.setattr(subprocess, "Popen", PopenMockFailRC)
    with pytest.raises(ImportError):
        TasksScheduler.check_ansible_version()
    logger.info("End")


def test_newer_openstack_version_pass(monkeypatch):
    logger = logging.getLogger(__name__)
    logger.info("Start")
    monkeypatch.setattr(subprocess, "Popen", PopenMockPass)
    TasksScheduler.check_openstack_version()
    logger.info("End")


def test_older_openstack_version_fails(monkeypatch):
    logger = logging.getLogger(__name__)
    logger.info("Start")
    monkeypatch.setattr(subprocess, "Popen", PopenMockFailOutput)
    with pytest.raises(ImportError):
        TasksScheduler.check_openstack_version()
    logger.info("End")


def test_openstack_not_installed_fails(monkeypatch):
    logger = logging.getLogger(__name__)
    logger.info("Start")
    monkeypatch.setattr(subprocess, "Popen", PopenMockFailRC)
    with pytest.raises(ImportError):
        TasksScheduler.check_openstack_version()
    logger.info("End")


@pytest.mark.timeout(600)
def test_stack_reuse(function_event_loop, manager_stack_reuse):
    logger = logging.getLogger(__name__)
    logger.info("Start")
    ok_path = create_script("script_ok.sh", "echo Dudu HaMelech")

    manager_stack_reuse.add_script_job("server1_public_ip", ok_path)
    manager_stack_reuse.add_script_job("server1_public_ip", ok_path)
    manager_stack_reuse.wait_all()
    delete_all_stacks(manager_stack_reuse)

    logger.info(manager_stack_reuse._stacks)
    assert manager_stack_reuse.get_stack_count() == 1

    logger.info("End")


@pytest.mark.incremental
class TestCreateSingleStackAndDestroyIt:
    @pytest.mark.asyncio
    async def test_stack_create(self, event_loop, manager_stack_0):
        logger = logging.getLogger(__name__)
        logger.info("Start")
        if delete_stack("scheduler1.test.pytest.stam"):
            logger.info("waiting for stack to be delted.")
            await asyncio.sleep(20)
        manager_stack_0.add_script_job("server1_public_ip", "/dev/null")
        await manager_stack_0._create_stack("scheduler1.test.pytest.stam")
        assert (
            manager_stack_0.get_stack_state("scheduler1.test.pytest.stam")
            == TasksScheduler.TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS
        )
        logger.info("End")

    @pytest.mark.asyncio
    async def test_stack_delete(self, event_loop, manager_stack_0):
        logger = logging.getLogger(__name__)
        logger.info("Start")
        retries = 20
        while (
            manager_stack_0.get_stack_state("scheduler1.test.pytest.stam")
            == TasksScheduler.TasksScheduler.STACK_STATUS_CREATE_IN_PROGRESS
        ):
            retries -= 1
            if not retries:
                break
            await asyncio.sleep(1)
            await manager_stack_0._update_stack_states()

        await manager_stack_0._delete_stack("scheduler1.test.pytest.stam")
        assert manager_stack_0.get_stack_state("scheduler1.test.pytest.stam") in (
            TasksScheduler.TasksScheduler.STACK_STATUS_DELETE_IN_PROGRESS,
            TasksScheduler.TasksScheduler.STACK_STATUS_DELETE_REQUESTED,
        )

        delete_all_stacks(manager_stack_0)
        logger.info("End")


@pytest.mark.timeout(600)
def test_dont_delete_on_error(
    function_event_loop, manager_stack_dont_delete_on_failure
):
    logger = logging.getLogger(__name__)
    logger.info("Start")
    fail_path = create_script("script_fail.sh", "exit 101")

    manager_stack_dont_delete_on_failure.add_script_job("server1_public_ip", fail_path)
    manager_stack_dont_delete_on_failure.wait_all()
    assert (
        manager_stack_dont_delete_on_failure.get_stack_count(
            TasksScheduler.TasksScheduler.STACK_STATUS_ERROR
        )
        == 1
    )

    delete_all_stacks(manager_stack_dont_delete_on_failure)
    logger.info("End")


def test_end_2_end(
    function_event_loop,
    manager_stack_e2e,
    fin_task_path,
    post_task_path,
    fin_task_path_local,
    post_task_path_local,
):
    global MANAGER

    logger = logging.getLogger(__name__)
    logger.info("Start")
    os.system("rm -f /tmp/*.out_os_scheduler")
    os.system("rm -f /tmp/*.err_os_scheduler")
    MANAGER = manager_stack_e2e

    if os.path.exists(e2e_out_folder):
        shutil.rmtree(e2e_out_folder)
    if os.path.exists(e2e_err_folder):
        shutil.rmtree(e2e_err_folder)

    os.makedirs(e2e_out_folder)
    os.makedirs(e2e_err_folder)

    ok_path = create_script(
        "script_ok.sh",
        "sleep 2 && sleep $((RANDOM % 5)) && echo Im Job $1 > /tmp/$1.out_os_scheduler",
    )
    fail_path = create_script(
        "script_fail.sh",
        "sleep 2 && sleep $((RANDOM % 5)) && echo Im Job $1 > /tmp/$1.err_os_scheduler; exit 1",
    )
    stacks_2_create = 10
    for job_i in range(stacks_2_create):
        if job_i % 3:
            path = ok_path
        else:
            path = fail_path

        manager_stack_e2e.add_script_job(
            "server1_public_ip", path, fin_task_path, post_task_path, parameters=[job_i]
        )

    manager_stack_e2e.add_script_job(
        "127.0.0.1",
        ok_path,
        fin_task_path_local,
        post_task_path_local,
        parameters=[stacks_2_create],
    )

    manager_stack_e2e.wait_all()

    total_outputs = sum([len(files) for r, d, files in os.walk(e2e_out_folder)])
    total_errors = sum([len(files) for r, d, files in os.walk(e2e_err_folder)])
    # total_outputs = len(get_file_list_recursive(e2e_out_folder))
    # total_errors = len(get_file_list_recursive(e2e_err_folder))
    total_jobs = manager_stack_e2e.get_job_total_count()

    assert total_outputs + total_errors == total_jobs
    assert e2e_job_cb_count == total_jobs
    assert e2e_job_cb_error == total_errors

    delete_all_stacks(manager_stack_e2e)
    logger.info("End")


def test_no_retries_on_create_failure(function_event_loop, manager_bad_flavour):
    """
    We test that we give up on retirying creating stacks that are failing with errors.
    The only error we ignore is no hosts found error because retires can actually succeed.
    :param manager_bad_flavour:
    :return:
    """
    logger = logging.getLogger(__name__)
    script_path = create_script("job.sh", "sleep 1")

    manager_bad_flavour.add_script_job("server1_public_ip", script_path)
    manager_bad_flavour.wait_all()
    assert (
        manager_bad_flavour.get_stack_count(
            TasksScheduler.TasksScheduler.STACK_STATUS_ERROR
        )
        == 1
    )

    delete_all_stacks(manager_bad_flavour)


def test_terminate_cmd(function_event_loop, rabbit_channel, manager_with_rabbit):
    message = {"cmd_type": "terminate", "args": {}}
    rabbit_channel.basic_publish(
        exchange="SchedulerCMDX", routing_key="", body=json.dumps(message)
    )

    script_path = create_script("job.sh", "sleep 1")

    manager_with_rabbit.add_script_job("server1_public_ip", script_path)
    manager_with_rabbit.wait_all()
    assert manager_with_rabbit._terminate_loop.is_set()
    assert manager_with_rabbit._terminate_reason == "A command to terminate was sent."

    delete_all_stacks(manager_with_rabbit)


def test_stop_create_cmd(function_event_loop, rabbit_channel, manager_with_rabbit):
    message = {
        "cmd_type": "stop_create",
        "args": {"reason": "testing stop create command"},
    }
    rabbit_channel.basic_publish(
        exchange="SchedulerCMDX", routing_key="", body=json.dumps(message)
    )

    script_path = create_script("job.sh", "sleep 1")

    manager_with_rabbit.add_script_job("server1_public_ip", script_path)
    asyncio.ensure_future(send_terminate(rabbit_channel), loop=function_event_loop)
    manager_with_rabbit.wait_all()
    assert manager_with_rabbit._terminate_loop.is_set()
    assert manager_with_rabbit._terminate_reason == "A command to terminate was sent."
    assert manager_with_rabbit.get_stack_count() == 0


@pytest.mark.timeout(600)
def test_duplicate_namespace(
    template_path,
    function_event_loop,
    manager_duplicate_schedulerA,
    manager_duplicate_schedulerB,
):
    logger = logging.getLogger(__name__)
    logger.info("Start")
    script_path = create_script("script.sh", "exit 0")

    manager_duplicate_schedulerA.add_script_job("server1_public_ip", script_path)
    manager_duplicate_schedulerB.add_script_job("server1_public_ip", script_path)

    manager_duplicate_schedulerA.wait_all()

    assert manager_duplicate_schedulerA._terminate_loop.is_set()
    assert (
        manager_duplicate_schedulerA._terminate_reason
        == "Done with the main task _update_states."
    )

    assert manager_duplicate_schedulerB._terminate_loop.is_set()
    assert manager_duplicate_schedulerB._terminate_reason.startswith(
        "Duplicate namespace prefix detected"
    )


@pytest.mark.timeout(600)
def test_stack_debug(function_event_loop, manager_stack_debug):
    global visited
    logger = logging.getLogger(__name__)
    logger.info("Start")
    script1 = create_script("a.sh", "echo Dudu HaMelech")
    script2 = create_script("b.sh", "exit 1")

    create_stack("my_own_stack", manager_stack_debug._template_path)

    manager_stack_debug.add_cmd_job("127.0.0.1", "{0} && {1}".format(script1, script2))

    visited = False

    def foo(jobid, rc):
        global visited
        stack_name = manager_stack_debug.get_stack(jobid)

        job = manager_stack_debug.get_stack_job(stack_name)

        assert rc == 0
        assert job["results"]["rc"] == 1
        visited = True

    manager_stack_debug._user_job_done_cb = foo

    manager_stack_debug.wait_all()
    assert manager_stack_debug.get_stack_count() == 1
    assert (
        manager_stack_debug.get_stack_count(
            TasksScheduler.TasksScheduler.STACK_STATUS_ERROR
        )
        == 0
    )
    assert visited == True

    delete_all_stacks(manager_stack_debug)
