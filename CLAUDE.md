# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Python-based process mining engine. Early stage — no source code exists yet. The `.gitignore` establishes the expected tooling conventions below.

## Expected Tooling (from .gitignore)

- **Linting/formatting**: `ruff`
- **Type checking**: `mypy`
- **Testing**: `pytest`
- **Package management**: `uv` (preferred, per `.gitignore` comment ordering)
- **Notebooks**: `marimo`

Once a `pyproject.toml` is added, commands will likely follow this pattern:
```
uv run ruff check .
uv run ruff format .
uv run mypy .
uv run pytest
uv run pytest tests/path/to/test_file.py::test_name  # single test
```
