#!/bin/bash
CUR_DIR="$(cd "$(dirname "$0")" && pwd)"

set -o pipefail


usage()
{
cat << EOF
    Usage: run_pytest.sh [--color] [--base_log_dir (the default is /tmp)]
           [--specific_test <test_name>]
           [--module_name <module_name> (the default is just the current directory)]
           [--jenkins_user <username>]
           [-h or --help for this usage message]
    Default values: --base_log_dir='/tmp'
EOF
}

color='no'
base_log_dir='/tmp/'
specific_test='none'
module_name=${CUR_DIR}
jenkins_user='none'

while [ "$1" != "" ]; do
    case $1 in
        --base_log_dir )   shift
                           base_log_dir=$1
                           ;;
        --module_name )    shift
                           module_name=$1
                           ;;
        --specific_test )   shift
                           specific_test=$1
                           ;;
        --jenkins_user )   shift
                           jenkins_user=$1
                           ;;
        --color )          color='yes'
                           ;;
        -h | --help )      usage
                           exit 0
                           ;;
        * )                usage
                           exit 1
    esac
    shift
done


COLOR=''
if [ ${color} = 'yes' ]; then
	COLOR='--color=yes'
fi

SPECIFIC_TEST=''
if [ ${specific_test} != 'none' ]; then
	SPECIFIC_TEST="-k ${specific_test}"
fi

DATE=$(date +%Y-%m-%d_%H-%M-%S)
OUT_PATH=${base_log_dir}

echo "Will run:"
echo "coverage3 run -m pytest -vv ${COLOR} ${SPECIFIC_TEST} \
-p no:cacheprovider \
--template_path ${CUR_DIR}/playbooks/test_stack.yml \
--ready_task_path ${CUR_DIR}/playbooks/test_stack_ready.yml \
--fin_task_path ${CUR_DIR}/playbooks/test_stack_fin.yml \
--post_task_path ${CUR_DIR}/playbooks/test_stack_post.yml \
--fin_local_task_path ${CUR_DIR}/playbooks/test_stack_fin_local.yml \
--post_local_task_path ${CUR_DIR}/playbooks/test_stack_post_local.yml \
--html=${OUT_PATH}/os_tasks_scheduler_system_tests_report-${DATE}.html \
--junit-xml=${OUT_PATH}/os_tasks_scheduler_system_tests_results.xml \
${module_name} | \
tee ${OUT_PATH}/os_tasks_scheduler_system_tests_output.log"

PYTHONPATH=${CUR_DIR} \
coverage3 run -m pytest -vv ${COLOR} ${SPECIFIC_TEST} \
-p no:cacheprovider \
--template_path ${CUR_DIR}/playbooks/test_stack.yml \
--ready_task_path ${CUR_DIR}/playbooks/test_stack_ready.yml \
--fin_task_path ${CUR_DIR}/playbooks/test_stack_fin.yml \
--post_task_path ${CUR_DIR}/playbooks/test_stack_post.yml \
--fin_local_task_path ${CUR_DIR}/playbooks/test_stack_fin_local.yml \
--post_local_task_path ${CUR_DIR}/playbooks/test_stack_post_local.yml \
--html=${OUT_PATH}/os_tasks_scheduler_system_tests_report-${DATE}.html \
--junit-xml=${OUT_PATH}/os_tasks_scheduler_system_tests_results.xml \
${module_name} | \
tee ${OUT_PATH}/os_tasks_scheduler_system_tests_output.log

RET_CODE=$?

exit ${RET_CODE}
