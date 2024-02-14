#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import logging
import os

from typing import Any

ATTRIBUTES_FILEPATH = os.path.join(os.path.dirname(__file__), "metadata", "attributes.csv")
INDICATOR_LIST = ["litpop", "crop-production", "earthquake", "flood"]
HAS_TIMESERIES = ["earthquake", "flood"]
NO_DATA = {}
NO_DATA["earthquake"] = set(["Burkina Faso", "Chad", "Niger", "Nigeria"])
NO_DATA["flood"] = set(
    [
        "Burkina Faso",
        "Cameroon",
        # "Colombia",  # time series missing
        "South Sudan",
        # "Sudan",  # time series missing
        "State of Palestine",
        # "Nigeria",  # time series missing
        # "Venezuela",  # time series missing
    ]
)


def get_set_of_countries_in_summary_file(summary_file_path: str, indicator: str) -> set:
    summary_countries = set()
    if os.path.exists(summary_file_path):
        with open(summary_file_path, encoding="utf-8") as summary_file:
            rows = csv.DictReader(summary_file)
            summary_countries = {x["country_name"] for x in rows if x["country_name"] != "#country"}
            summary_countries = summary_countries.union(NO_DATA.get(indicator, set()))

    return summary_countries


def write_dictionary(
    output_filepath: str, output_rows: list[dict[str, Any]], append: bool = True
) -> str:
    if len(output_rows) == 0:
        status = "No data provided to write_dictionary"
        return status
    keys = list(output_rows[0].keys())
    newfile = not os.path.isfile(output_filepath)

    if not append and not newfile:
        os.remove(output_filepath)
        newfile = True

    with open(output_filepath, "a", encoding="utf-8", errors="ignore") as output_file:
        dict_writer = csv.DictWriter(
            output_file,
            keys,
            lineterminator="\n",
        )
        if newfile:
            dict_writer.writeheader()
        dict_writer.writerows(output_rows)

    status = _make_write_dictionary_status(append, output_filepath, newfile)

    return status


def _make_write_dictionary_status(append: bool, filepath: str, newfile: bool) -> str:
    status = ""
    if not append and not newfile:
        status = f"Append is False, and {filepath} exists therefore file is being deleted"
    elif not newfile and append:
        status = f"Append is True, and {filepath} exists therefore data is being appended"
    else:
        status = f"New file {filepath} is being created"
    return status


def read_attributes(dataset_name: str) -> dict:
    with open(ATTRIBUTES_FILEPATH, "r", encoding="UTF-8") as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)

        attributes = {}
        for row in attribute_rows:
            if row["dataset_name"] != dataset_name:
                continue
            if row["attribute"] in ["resource", "skip_country"]:
                if row["attribute"] not in attributes:
                    attributes[row["attribute"]] = [row["value"]]
                else:
                    attributes[row["attribute"]].append(row["value"])
            else:
                attributes[row["attribute"]] = row["value"]

    return attributes


def read_countries():
    with open(
        os.path.join(os.path.dirname(__file__), "metadata", "countries.csv"), encoding="utf-8"
    ) as countries_file:
        rows = list(csv.DictReader(countries_file))

    return rows


def print_banner_to_log(logger: logging.Logger, name: str):
    title = f"Climada - {name}"
    timestamp = f"Invoked at: {datetime.datetime.now().isoformat()}"
    width = max(len(title), len(timestamp))
    logger.info((width + 4) * "*")
    logger.info(f"* {title:<{width}} *")
    logger.info(f"* {timestamp:<{width}} *")
    logger.info((width + 4) * "*")
