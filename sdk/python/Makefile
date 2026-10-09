.PHONY: install install-dev format lint mypy test audit

install:  ## Install the dependencies excluding dev.
	poetry install --only main

install-dev:  ## Install the dependencies including dev.
	poetry install

format:  ## Format the code.
	poetry run ruff check . --fix
	poetry run ruff format .

lint:  ## Run ruff and mypy checks.
	poetry run ruff check .
	poetry run ruff format --check .
	$(MAKE) mypy

mypy:  ## Run mypy.
	poetry run mypy dis_dataset_api_sdk_python

test:  ## Run tests and check coverage.
	poetry run pytest -n auto --cov=dis_dataset_api_sdk_python --cov-report=term-missing --cov-fail-under=80

audit:  ## Run pip-audit to check for vulnerabilities.
	poetry run pip-audit
