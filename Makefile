NAME=os_task_scheduler
DIR_NAME=os_task_scheduler
PIP := pip3
COVERAGE := coverage3

.PHONY: build _build check-local dist check docs major _major minor _minor patch _patch _debug _publish check-local

help:
	@echo "COMMANDS:"
	@echo "  clean          Remove all generated files."
	@echo "  test           Run tests."
	@echo "  check          Run PEP8 analysis tools."
	@echo "  major        	Build major versoin X.O.O and upload package"
	@echo "  minor        	Build minor versoin O.X.O and upload package"
	@echo "  patch        	Build patch versoin O.O.X and upload package"

clean:
	@echo "clean: Remove old files"
	rm -rf ${PWD}/dist
	rm -rf ${PWD}/*.egg-info
	rm -rf ${PWD}/venv
	rm -rf ${PWD}/.coverage
	rm -rf ${PWD}/.pytest_cache
	rm -rf ${PWD}/examples/*.log*
	rm -rf ${PWD}/tests/*.log*


test_ci:
	$(COVERAGE) erase
	${PWD}/tests/run_pytest.sh --jenkins_user ${JENKINS_USER} --base_log_dir ${BASE_LOG_DIR}
	$(COVERAGE) report report ${DIR_NAME}/*.py

test:
	$(COVERAGE) erase
	${PWD}/tests/run_pytest.sh
	$(COVERAGE) report report ${DIR_NAME}/*.py

check: check-local

check-local:
	@echo "-------------------------------------------------------------"
	@echo "-------------------------------------------------------------"
	@echo "-~      Running static checks                              --"
	@echo "-------------------------------------------------------------"
	PYTHONPATH=${PWD} flake8 --version
	PYTHONPATH=${PWD} flake8 __pycache__ \
        --exclude version.py \
	--ignore E501,W503 ${PWD}/${DIR_NAME}/
	@echo "-------------------------------------------------------------"
	@echo "-~      Running unit tests                                 --"
	@echo "-------------------------------------------------------------"
	@echo "-------------------------------------------------------------"
	@echo "-------------------------------------------------------------"

build: check  _debug _build

_build:
	@echo "Building (Copying version file)"

_publish: clean
	@echo "Publishing"
	cp ${PWD}/../versions/${NAME}/version.py ${PWD}/${NAME}/
	python3 setup.py sdist bdist_wheel
	twine upload ${PWD}/dist/*
	git checkout -- ${PWD}/${NAME}/version.py

major: check _major _build _publish

_major:
	@echo "Major Release"
	$(eval VERSION := $(shell version_manager \
	--repos_path ${PWD}/../ \
	--app_version_file ${PWD}/../versions/libs/python3/${NAME}/version.py \
	--release_mode major --app_name ${NAME}))

minor: check _minor _build _publish

_minor:
	@echo "Minor Release"
	$(eval VERSION := $(shell version_manager \
	--repos_path ${PWD}/../ \
	--app_version_file ${PWD}/../versions/libs/python3/${NAME}/version.py \
	--release_mode minor --app_name ${NAME}))

patch: check _patch _build _publish

_patch:
	@echo "Patch Release"
	$(eval VERSION := $(shell version_manager \
	--repos_path ${PWD}/../ \
	--app_version_file ${PWD}/../versions/libs/python3/${NAME}/version.py \
	--release_mode patch --app_name ${NAME}))

_debug:
	@echo "Debug Release"
	$(eval VERSION := $(shell version_manager \
	--repos_path ${PWD}/../ \
	--app_version_file ${PWD}/../versions/libs/python3/${NAME}/version.py \
	--release_mode debug --app_name ${NAME}))
