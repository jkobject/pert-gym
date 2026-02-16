.ONESHELL:
ENV_PREFIX=$(shell python -c "if __import__('pathlib').Path('.venv/bin/pip').exists(): print('.venv/bin/')")

.PHONY: help
help:             ## Show the help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@fgrep "##" Makefile | fgrep -v fgrep

.PHONY: show
show:             ## Show the current environment.
	@echo "Current environment:"
	@echo "Running using $(ENV_PREFIX)"
	@$(ENV_PREFIX)python -V
	@$(ENV_PREFIX)python -m site

.PHONY: install
install:          ## Install the project in dev mode.
	$(ENV_PREFIX)uv sync --all-extras --dev

.PHONY: fmt
fmt:              ## Format source code.
	$(ENV_PREFIX)uv run --no-sync ruff format src tests

.PHONY: lint
lint:             ## Run linters.
	$(ENV_PREFIX)uv run --no-sync ruff check --fix src tests

.PHONY: type
type:             ## Run type checking (Astral ty).
	$(ENV_PREFIX)uv run --no-sync ty check src

.PHONY: test
test: lint type   ## Run tests and generate coverage report.
	set -e
	$(ENV_PREFIX)uv run --no-sync pytest -v -x --cov=src --cov-report=xml --cov-report=html --tb=short tests

.PHONY: watch
watch:            ## Run tests on every change.
	ls src/**/*.py tests/**/*.py | entr $(ENV_PREFIX)uv run --no-sync pytest -s -vv -l --tb=long --maxfail=1 tests

.PHONY: clean
clean:            ## Clean unused files.
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '__pycache__' -exec rm -rf {} \;
	@find ./ -name 'Thumbs.db' -exec rm -f {} \;
	@find ./ -name '*~' -exec rm -f {} \;
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf build
	@rm -rf dist
	@rm -rf *.egg-info
	@rm -rf htmlcov
	@rm -rf .tox/
	@rm -rf site

.PHONY: virtualenv
virtualenv:       ## Create a virtual environment.
	@echo "creating virtualenv ..."
	@rm -rf .venv
	@uv venv
	@source .venv/bin/activate
	@make install
	@echo "Run 'source .venv/bin/activate' to enable the environment."

.PHONY: release
release:          ## Create a new tag for release.
	@echo "WARNING: This operation will update version files and push a tag."
	@read -p "Remote name? (e.g. origin) : " REMOTE
	@read -p "Version? (provide the next x.y.z semver) : " TAG
	@echo "$${TAG}" > src/pert_gym/VERSION
	@$(ENV_PREFIX)python -c "from pathlib import Path; p=Path('pyproject.toml'); tag='$$TAG'; p.write_text('\\n'.join([f'version = \"{tag}\"' if line.startswith('version = ') else line for line in p.read_text().splitlines()]) + '\\n')"
	@$(ENV_PREFIX)uv run --no-sync gitchangelog > HISTORY.md
	@git add src/pert_gym/VERSION HISTORY.md pyproject.toml
	@git commit -m "release: version $${TAG}"
	@echo "creating git tag : $${TAG}"
	@git tag $${TAG}
	@git push -u $${REMOTE} HEAD --tag

.PHONY: docs
docs:             ## Build the documentation.
	@echo "building documentation ..."
	@$(ENV_PREFIX)uv run --no-sync mkdocs build
