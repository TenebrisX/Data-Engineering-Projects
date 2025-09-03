.PHONY: format lint test run

format:
	ruff --fix .
lint:
	ruff .
	mypy src
test:
	pytest -q
run:
	python -m src.main
