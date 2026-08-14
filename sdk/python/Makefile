
.PHONY: install test lint format typecheck

install:
	poetry install

test:
	poetry run python -m unittest discover -s tests -v

lint:
	poetry run ruff check .

format:
	poetry run ruff format .

typecheck:
	poetry run mypy dis_dataset_api_sdk_python

