.PHONY: setup lint test summary bench audit ci

PY ?= python

setup:
	$(PY) -m pip install --upgrade pip && $(PY) -m pip install -e ".[dev]"

# ruff, ruff format and mypy --strict.
lint:
	$(PY) -m ruff check . && $(PY) -m ruff format --check . && $(PY) -m mypy

# Event model, generator and reference engine; Kafka and Flink tests skip unless their CI jobs
# provide a broker (KAFKA_BOOTSTRAP) or PyFlink.
test:
	$(PY) -m pytest -q -rs

# Generate 100,000 events and summarise disorder, lateness and window results.
summary:
	clickgen --events 100000

bench:
	@echo "M3: kill-the-worker demo with identical output counts, and throughput and latency at several event rates"

# Known vulnerabilities in the installed Python dependencies.
audit:
	$(PY) -m pip_audit --skip-editable --cache-dir .tmp/pip-audit

ci: setup lint test
