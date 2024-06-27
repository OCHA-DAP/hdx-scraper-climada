SHELL := /bin/bash
install:
	echo "GDAL must be installed from pre-compiled binaries on Windows machines prior to pip install"
	pip install -e .
lint:
	black . --check
	flake8 --config=config/.flake8 src/
	pylint --rcfile=config/.pylintrc src/ || true
unit_tests:
	pytest -v --cov=hdx_scraper_climada --cov-config=config/.coveragerc tests/
timed_tests:
	pytest -v --durations=0 --durations-min=1 --cov=hdx_scraper_climada --cov-config=config/.coveragerc tests/
github_tests:
	pytest -m "not local_only" -v --durations=0 --durations-min=1 --cov=hdx_scraper_climada --cov-config=config/.coveragerc tests
run:
	python src/hdx_scraper_climada/run.py
