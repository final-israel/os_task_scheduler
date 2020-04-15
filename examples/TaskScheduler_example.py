import os_task_scheduler.TasksScheduler as TasksScheduler
import os
import shutil
import logging
import logging.handlers

MANAGER = None


def job_done_cb(jobid, rc):
    """
    Function that will be called when ever a job terminates.
    """
    logger = logging.getLogger(__name__)
    stack_name = MANAGER.get_stack(jobid)
    public_ip = MANAGER.get_stack_attribute(stack_name, "server2_public_ip")
    job = MANAGER.get_stack_job(stack_name)

    logger.info(
        "Job {} done with rc {} and my public is {}.".format(jobid, rc, public_ip)
    )
    logger.info("Job {} results {}".format(jobid, job["results"]))


def create_script(name, cmd_list):
    """
    We generate our remote bash scripts dynamically for testability.
    In most cases it will be practictical to have static scripts (each script run a group of fixed tests...)
    """
    full_path = os.path.join(os.path.expanduser("~"), name)
    with open(full_path, "w") as f:
        print("#!/bin/bash", file=f)
        print("set -e", file=f)
        for cmd in cmd_list:
            print(cmd, file=f)

    return full_path


def main():
    global MANAGER
    logger = logging.getLogger(__name__)
    # we init our manger with definitions for:
    #   1. What our stack looks like?
    #   2. When it is ready to run scripts?
    #   3. Which node in it will be responsible to declare stuck ready?
    #   4. What user\passwd to use.
    #   5. What action to preforme when the script has finished.
    manager = TasksScheduler.TasksScheduler(
        template_path="../tests/playbooks/test_stack.yml",
        ready_tasks_file_path="../tests/playbooks/test_stack_ready.yml",
        access_point_node_ref="server1_public_ip",
        name_prefix="schedexmpl",
        ssh_user="tstuser",
        ssh_password="tstpass",
        job_done_cb=job_done_cb,
        stack_reuse=True,
        stack_create_params={"private_net_name": "private_net_scheduler"},
        stack_limit=40,
    )

    MANAGER = manager
    # The local place where we expect script outputs to arrive (defined in test_stack_fin.yml)
    home = os.path.expanduser("~")
    test_out_folder = f"{home}/test_end_2_end/outputs"
    if os.path.exists(test_out_folder):
        shutil.rmtree(test_out_folder)

    os.makedirs(test_out_folder)

    jobs_2_create = 10
    # we create a simple script that will represent the job.
    # Note:
    #   1. $1 is the first parameter passed to the job.
    #   2. $server2_public_ip will be resolved to the value of the matching output value as described in
    # the stack template file.
    script_path = create_script(
        "script.sh",  # Our script just sleep and echo their id and server2 IP
        [
            "sleep $((RANDOM % 60))",
            "echo Im Job $1 and server2_public_ip is $server2_public_ip > /var/tstdir/$1.out_os_scheduler",
            "exit 0",
        ],
    )

    for job_i in range(
        jobs_2_create
    ):  # we generate scripts dynamically. You probably have static number of known jobs...
        manager.add_script_job(
            "server1_public_ip",
            script_path,
            "../tests/playbooks/test_stack_fin.yml",
            parameters=[job_i],
        )

    # thats it just wait for all to finish
    manager.wait_all()

    # dont forget to do somthing useful with the jobs outputs...
    for r, d, files in os.walk(test_out_folder):
        for file in files:
            path = os.path.join(r, file)
            with open(path) as f:
                logger.info("{}:".format(path))
                logger.info(f.read())


def logger_init():
    logger = logging.getLogger()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s"
    )
    logger.setLevel(logging.DEBUG)

    cons_handler = logging.StreamHandler()
    cons_handler.setLevel(logging.INFO)
    cons_handler.setFormatter(formatter)

    file_handler = logging.handlers.RotatingFileHandler(
        "tasksched.log", maxBytes=5 * 1024 * 1024, backupCount=2
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    logger.addHandler(cons_handler)
    logger.addHandler(file_handler)

    logger.info("Logger init")


if __name__ == "__main__":
    logger_init()
    logger = logging.getLogger()

    main()
    logger.info("Done")
