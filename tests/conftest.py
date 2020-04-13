import pytest
import logging
import logging.handlers
import os
import OSTaskScheduler.TasksScheduler as TasksScheduler


def pytest_addoption(parser):
    parser.addoption(
        "--template_path",
        action="store",
        default="./playbooks/test_stack.yml",
        help="path for test stack template to be used",
    )
    parser.addoption(
        "--ready_task_path",
        action="store",
        default="./playbooks/test_stack_ready.yml",
        help="path for test stack ready ansible tasks",
    )
    parser.addoption(
        "--fin_task_path",
        action="store",
        default="./playbooks/test_stack_fin.yml",
        help="path for test stack fin ansible tasks",
    )
    parser.addoption(
        "--post_task_path",
        action="store",
        default="./playbooks/test_stack_post.yml",
        help="path for test stack post ansible tasks",
    )
    parser.addoption(
        "--fin_local_task_path",
        action="store",
        default="./playbooks/test_stack_fin_local.yml",
        help="path for test stack fin ansible tasks",
    )
    parser.addoption(
        "--post_local_task_path",
        action="store",
        default="./playbooks/test_stack_post_local.yml",
        help="path for test stack post ansible tasks",
    )


@pytest.fixture(scope="session")
def template_path(request):
    path = request.config.getoption("--template_path")
    assert os.path.isfile(path)
    return os.path.abspath(path)


@pytest.fixture(scope="session")
def ready_task_path(request):
    path = request.config.getoption("--ready_task_path")
    assert os.path.isfile(path)
    return os.path.abspath(path)


@pytest.fixture(scope="session")
def fin_task_path(request):
    path = request.config.getoption("--fin_task_path")
    assert os.path.isfile(path)
    return os.path.abspath(path)


@pytest.fixture(scope="session")
def post_task_path(request):
    path = request.config.getoption("--post_task_path")
    assert os.path.isfile(path)
    return os.path.abspath(path)


@pytest.fixture(scope="session")
def fin_task_path_local(request):
    path = request.config.getoption("--fin_local_task_path")
    assert os.path.isfile(path)
    return os.path.abspath(path)


@pytest.fixture(scope="session")
def post_task_path_local(request):
    path = request.config.getoption("--post_local_task_path")
    assert os.path.isfile(path)
    return os.path.abspath(path)


@pytest.fixture(scope="session", autouse=True)
def logger_init(request):
    logger = logging.getLogger()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s"
    )
    logger.setLevel(logging.DEBUG)

    cons_handler = logging.StreamHandler()
    cons_handler.setLevel(logging.INFO)
    cons_handler.setFormatter(formatter)

    file_handler = logging.handlers.RotatingFileHandler(
        "pytest-jobmng.log", maxBytes=20 * 1024 * 1024, backupCount=1
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    logger.addHandler(cons_handler)
    logger.addHandler(file_handler)

    logger.info("Logger init")


def pytest_runtest_makereport(item, call):
    if "incremental" in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item


def pytest_runtest_setup(item):
    if "incremental" in item.keywords:
        previousfailed = getattr(item.parent, "_previousfailed", None)
        if previousfailed is not None:
            pytest.xfail("previous test failed (%s)" % previousfailed.name)


@pytest.fixture(scope="class")
def manager_stack_0(template_path, rabbit_service):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="scheduler1.test",
        rabbit_host="127.0.0.1",
    )
    manager._skip_check_duplicates = True
    return manager


@pytest.fixture
def manager_stack_dont_delete_on_failure(
    template_path, ready_task_path, post_task_path, rabbit_service
):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        ready_tasks_file_path=ready_task_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="scheduler3.test",
        ssh_user="tstuser",
        ssh_password="tstpass",
        rabbit_host="127.0.0.1",
    )
    manager._skip_check_duplicates = True
    return manager


@pytest.fixture
def manager_bad_flavour(template_path, ready_task_path, post_task_path, rabbit_service):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        ready_tasks_file_path=ready_task_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="scheduler4.test",
        ssh_user="qa",
        ssh_password="852456",
        extra_stack_create_params={"default_flavor": "non_existing_flavor"},
        stack_limit=1,
        rabbit_host="127.0.0.1",
    )

    manager._skip_check_duplicates = True
    return manager


@pytest.fixture
def manager_duplicate_schedulerA(
    template_path, ready_task_path, post_task_path, rabbit_service
):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="manager_duplicate_scheduler",
        ssh_user="tstuser",
        ssh_password="tstpass",
        rabbit_host="127.0.0.1",
    )
    manager._skip_check_duplicates = True
    return manager


@pytest.fixture
def manager_with_rabbit(template_path, ready_task_path, post_task_path, rabbit_service):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        ready_tasks_file_path=ready_task_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="scheduler5.test",
        ssh_user="tstuser",
        ssh_password="tstpass",
        rabbit_host="127.0.0.1",
    )

    manager._skip_check_duplicates = True
    return manager


@pytest.fixture
def manager_stack_reuse(template_path, ready_task_path, post_task_path, rabbit_service):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        ready_tasks_file_path=ready_task_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="scheduler6.test",
        ssh_user="tstuser",
        ssh_password="tstpass",
        stack_reuse=True,
        stack_limit=1,
        rabbit_host="127.0.0.1",
    )

    manager._skip_check_duplicates = True
    return manager


@pytest.fixture
def manager_duplicate_schedulerA(template_path, rabbit_service):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="manager_duplicate_scheduler",
        ssh_user="tstuser",
        ssh_password="tstpass",
        rabbit_host="127.0.0.1",
    )

    manager._skip_check_duplicates = True
    return manager


@pytest.fixture
def manager_duplicate_schedulerB(template_path, rabbit_service):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="manager_duplicate_scheduler",
        ssh_user="tstuser",
        ssh_password="tstpass",
        rabbit_host="127.0.0.1",
    )

    return manager


@pytest.fixture
def manager_stack_debug(template_path, ready_task_path, post_task_path, rabbit_service):
    manager = TasksScheduler.TasksScheduler(
        template_path=template_path,
        access_point_node_ref="server1_public_ip",
        name_prefix="debug_prefix",
        ssh_user="tstuser",
        ssh_password="tstpass",
        rabbit_host="127.0.0.1",
        debug_stack="my_own_stack",
    )

    manager._skip_check_duplicates = True
    return manager
