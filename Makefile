.PHONY: help
help:	## This help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## Remove build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## Remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## Remove test and coverage artifacts
	rm -rf .tox/
	rm -f .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache

# install: clean  ## Install in a current environment
# 	python setup.py install

install-dev: ## Install in development mode
	pip install -e ".[dev]"

uninstall-dev: ## Uninstall development packages
	pip freeze > requirements.txt
	pip uninstall -r requirements.txt -y || true
	rm requirements.txt


build: clean ## Build wheel
	python -m build