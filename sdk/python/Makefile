.PHONY: install test test-cov lint format typecheck audit

install:
	poetry install

test:
	poetry run pytest tests/ -v

test-cov:
	poetry run pytest tests/ -v --cov=dis_dataset_api_sdk_python --cov-report=term-missing

lint:
	poetry run ruff check .

format:
	poetry run ruff format .

typecheck:
	poetry run mypy dis_dataset_api_sdk_python

audit:
	poetry run pip-audit
