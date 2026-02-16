# pert-gym

[![CI](https://github.com/your-org/pert-gym/actions/workflows/main.yml/badge.svg)](https://github.com/your-org/pert-gym/actions/workflows/main.yml)
[![PyPI version](https://badge.fury.io/py/pert-gym.svg)](https://badge.fury.io/py/pert-gym)

Reusable Python package scaffold with a modern development workflow.

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Development](#development)
- [Project Structure](#project-structure)
- [Docker](#docker)
- [Contributing](#contributing)

## Install

```bash
uv venv .venv --python 3.11
source .venv/bin/activate
make install
```

## Usage

```bash
pert-gym --help
pert-gym run --config config/base.yml
```

## Development

```bash
make fmt
make lint
make type
make test
make docs
```

## Project Structure

```text
.
├── .github/
│   ├── ISSUE_TEMPLATE/
│   └── workflows/
├── config/
├── data/
│   ├── main/
│   └── others/
├── docs/
├── figures/
├── notebooks/
├── src/pert_gym/
├── tests/
├── tools/
├── Dockerfile
├── HISTORY.md
├── LICENSE
├── Makefile
├── mkdocs.yml
└── pyproject.toml
```

## Docker

```bash
docker build -t pert-gym:latest .
docker run --rm -it pert-gym:latest
```

## Contributing

1. Open an issue or feature request.
2. Create a branch.
3. Run `make lint type test` before opening a PR.
