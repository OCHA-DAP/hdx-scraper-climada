SHELL := /bin/bash
install:
	echo "GDAL must be installed from pre-compiled binaries on Windows machines prior to pip install"
	pip install -e .
download_assets:
	python src/hdx_scraper_climada/download_admin_geometries_from_hdx.py
	python src/hdx_scraper_climada/download_gpw_population_map.py
lint:
	black . --check
	flake8 --config=config/.flake8 src/
	pylint --rcfile=config/.pylintrc src/ || true
unit_tests:
	pytest -v --cov=hdx_scraper_climada --cov-config=config/.coveragerc tests/
timed_tests:
	pytest -v --durations=0 --durations-min=1 --cov=hdx_scraper_climada --cov-config=config/.coveragerc tests/
github_tests:
	pytest -m "not local_only" -v --cov=hdx_scraper_climada --cov-config=config/.coveragerc tests
run:
	python src/hdx_scraper_climada/run.py
