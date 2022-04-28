# Makefile dataeng-astronomer-evergreen-enterprise

container_base_tag=astronomer-evergreen-enterprise

# Variables assigned with a "placeholder" value are going to be retrieved from CI secrets
astronomer_api_key="placeholder"

# keys to login to a specific docker hub (docker login -p {{ }})
# from service accounts
astro_staging_key="placeholder"
astro_prod_key="placeholder"
gh_token="placeholder"

docker_registry_path=registry.astronomer.condenast.io

#corresponds to the release name in astronomer (for deployments)
release_name_staging=earthbound-ionization-0859
release_name_prod=flickering-corona-0607
build_number="1"

python_exec=$(shell command -v python3)
venv_path=$(shell pwd)/app_env
current_dir=$(shell pwd)

create_venv:
	@echo Creating venv with [${python_exec}] at [${venv_path}]
	${python_exec} -m venv ${venv_path}

install_venv_deps:
	@echo Installing venv deps
	${python_exec} -m pip install --upgrade pip
	pip install -r requirements.txt

destroy_venv:
	@echo Destroying directory [${venv_path}]
	rm -dfr ${venv_path}

activate_venv:
	@echo Execute: source ${venv_path}/bin/activate

deactivate_venv:
	@echo Execute: [deactivate]
	deactivate

local_build:
	DOCKER_BUILDKIT=1 docker build -t ${container_base_tag} .

local_lint_code:
	@echo Checking the format of all .py files in the directory hierarchy
	find ./* -name "*.py" -not -path "./app_env/*" ! -name "__init__.py" | xargs flake8

check_code_format:
	@echo Checking the format of all .py files in the directory hierarchy
	apk add --no-cache python3 py3-pip
	pip install --upgrade flake8
	find ./* -name "*.py" -not -path "./app_env/*" ! -name "__init__.py" | xargs flake8

ci_cd_staging: check_code_format
	@echo DATABRICKS_URL ${databricks_url}
	@echo Building and deploying [${docker_registry_path}/${release_name_staging}/airflow:ci-${build_number}]
	docker login ${docker_registry_path} -u _ -p "${astro_staging_key}"
	docker build -t ${docker_registry_path}/${release_name_staging}/airflow:ci-${build_number} . --build-arg GH_TOKEN=${gh_token}
	docker push ${docker_registry_path}/${release_name_staging}/airflow:ci-${build_number}

ci_cd_prod: check_code_format
	@echo Building and deploying [${docker_registry_path}/${release_name_prod}airflow:ci-${build_number}]
	docker login ${docker_registry_path} -u _ -p "${astro_prod_key}"
	docker build -t ${docker_registry_path}/${release_name_prod}/airflow:ci-${build_number} . --build-arg GH_TOKEN=${gh_token}
	docker push ${docker_registry_path}/${release_name_prod}/airflow:ci-${build_number}

start_astro_local:
	astro dev start

stop_astro_local:
	astro dev stop

kill_astro_local:
	astro dev kill
