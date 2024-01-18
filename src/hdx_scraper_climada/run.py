#!/usr/bin/env python
# encoding: utf-8

import datetime
import logging
import os
import time

from hdx.utilities.easy_logging import setup_logging

from hdx_scraper_climada.create_csv_files import (
    export_indicator_data_to_csv,
    make_detail_and_summary_file_paths,
)
from hdx_scraper_climada.create_datasets import create_datasets_in_hdx
from hdx_scraper_climada.utilities import read_countries, print_banner_to_log

setup_logging()
LOGGER = logging.getLogger(__name__)


def check_csv_files(indicator: str):
    countries = [x["country_name"] for x in read_countries()]
    countries_to_process = []
    for country in countries:
        detail_file_path, _ = make_detail_and_summary_file_paths(country, indicator)
        # check which detail files exist
        if os.path.exists(detail_file_path):
            continue
        countries_to_process.append(country)

    # Check which countries are in the summary

    return countries_to_process


def process_list(countries_to_process: list[dict], indicator: str, dry_run: bool = True):
    dataset_name = f"climada-{indicator}-dataset"
    for country in countries_to_process:
        statuses = export_indicator_data_to_csv(country=country, indicator=indicator)
        for status in statuses:
            LOGGER.info(status)
    create_datasets_in_hdx(dataset_name, dry_run=dry_run)

    pass


def create_csv_files():
    pass


def create_datasets():
    pass


if __name__ == "__main__":
    INDICATOR = "litpop"
    DRY_RUN = True
    T0 = time.time()
    print_banner_to_log(LOGGER, "Updating Climada Datasets")

    COUNTRIES_TO_PROCESS = check_csv_files(INDICATOR)

    LOGGER.info(COUNTRIES_TO_PROCESS)
    # process_list(COUNTRIES_TO_PROCESS, INDICATOR, dry_run=DRY_RUN)

    LOGGER.info(f"Processed all countries in {time.time()-T0:0.0f} seconds")
    LOGGER.info(f"Timestamp: {datetime.datetime.now().isoformat()}")
