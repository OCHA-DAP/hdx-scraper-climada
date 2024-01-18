#!/usr/bin/env python
# encoding: utf-8

import csv
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


def check_csv_files(indicator: str) -> set:
    all_countries = {x["country_name"] for x in read_countries()}
    # Check which countries are in the summary
    _, summary_file_path = make_detail_and_summary_file_paths("Haiti", indicator)

    if os.path.exists(summary_file_path):
        with open(summary_file_path, encoding="utf-8") as summary_file:
            rows = csv.DictReader(summary_file)
            summary_countries = {x["country_name"] for x in rows if x["country_name"] != "#country"}
    else:
        summary_countries = set()

    detail_countries = set()
    # check which detail files exist
    for country in all_countries:
        detail_file_path, _ = make_detail_and_summary_file_paths(country, indicator)

        if os.path.exists(detail_file_path):
            detail_countries.update([country])

    missing_detail_countries = all_countries.difference(detail_countries)
    missing_summary_countries = all_countries.difference(summary_countries)
    if missing_summary_countries != missing_detail_countries:
        LOGGER.info("WARNING: Detail file and summary files inconsistent")
        LOGGER.info(
            "Countries in summary not in detail: "
            f"{missing_summary_countries.difference(missing_detail_countries)}"
        )
        LOGGER.info(
            "Countries in detail not in summary: "
            f"{missing_detail_countries.difference(missing_summary_countries)}"
        )

    countries_to_process = missing_detail_countries.union(missing_summary_countries)

    countries_to_process = sorted(list(countries_to_process))

    return countries_to_process


def process_list(countries_to_process: list[dict], indicator: str, dry_run: bool = True):
    dataset_name = f"climada-{indicator}-dataset"
    for country in countries_to_process:
        statuses = export_indicator_data_to_csv(country=country, indicator=indicator)
        for status in statuses:
            LOGGER.info(status)
    create_datasets_in_hdx(dataset_name, dry_run=dry_run)


if __name__ == "__main__":
    INDICATOR = "litpop"
    DRY_RUN = True
    T0 = time.time()
    print_banner_to_log(LOGGER, "Updating Climada Datasets")

    COUNTRIES_TO_PROCESS = check_csv_files(INDICATOR)

    LOGGER.info(COUNTRIES_TO_PROCESS)
    process_list(COUNTRIES_TO_PROCESS, INDICATOR, dry_run=DRY_RUN)

    LOGGER.info(f"Processed all countries in {time.time()-T0:0.0f} seconds")
    LOGGER.info(f"Timestamp: {datetime.datetime.now().isoformat()}")