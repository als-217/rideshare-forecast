.PHONY: help setup lint format typecheck test check clean

help:
	@echo "Targets:"
	@echo "  setup      install dependencies and git hooks"
	@echo "  lint       run ruff lint checks"
	@echo "  format     auto-format code with ruff"
	@echo "  typecheck  run pyright"
	@echo "  test       run pytest"
	@echo "  check      run lint, typecheck and tests (the CI gate)"
	@echo "  clean      remove caches"

setup:
	uv sync
	uv run pre-commit install

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check . --fix
	uv run ruff format .

typecheck:
	uv run pyright

test:
	uv run pytest

check: lint typecheck test

clean:
	rm -rf .pytest_cache .ruff_cache .pyright __pycache__
