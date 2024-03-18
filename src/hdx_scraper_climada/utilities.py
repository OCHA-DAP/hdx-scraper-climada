#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import logging
import os

from typing import Any

ATTRIBUTES_FILEPATH = os.path.join(os.path.dirname(__file__), "metadata", "attributes.csv")
INDICATOR_LIST = [
    "litpop",
    "crop-production",
    "earthquake",
    "flood",
    "wildfire",
    "river-flood",
    "tropical-cyclone",
    "storm-europe",
]
HAS_TIMESERIES = [
    "earthquake",
    "flood",
    "wildfire",
    "river-flood",
    "tropical-cyclone",
    "storm-europe",
]

NO_DATA = {}
NO_DATA["litpop"] = set(["Syrian Arab Republic"])
NO_DATA["earthquake"] = set(["Burkina Faso", "Chad", "Niger", "Nigeria"])
NO_DATA["flood"] = set(
    [
        "Burkina Faso",
        "Cameroon",
        "South Sudan",
        "State of Palestine",
    ]
)

NO_DATA["tropical-cyclone"] = set(
    [
        "Afghanistan",
        "Burkina Faso",
        "Burundi",
        "Cameroon",
        "Central African Republic",
        "Chad",
        "DR Congo",
        "Mali",
        "Niger",
        "Nigeria",
        "South Sudan",
        "State of Palestine",
        "Sudan",
        "Syrian Arab Republic",
        "Ukraine",
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
    """A function for reading attributes from a standard attributes.csv file with columns:
    dataset_name,timestamp,attribute,value,secondary_value

    Arguments:
        dataset_name {str} -- the name of the dataset for which attributes are required

    Returns:
        dict -- a dictionary containing the attributes
    """
    with open(ATTRIBUTES_FILEPATH, "r", encoding="UTF-8") as attributes_filehandle:
        attribute_rows = csv.DictReader(attributes_filehandle)

        attributes = {}
        for row in attribute_rows:
            if row["dataset_name"] != dataset_name:
                continue
            if row["attribute"] in ["resource", "skip_country", "showcase", "tags"]:
                if row["attribute"] not in attributes:
                    attributes[row["attribute"]] = [row["value"]]
                else:
                    attributes[row["attribute"]].append(row["value"])
            else:
                attributes[row["attribute"]] = row["value"]

        if attributes and "name" not in attributes:
            attributes["name"] = dataset_name

    documentation_dict = read_documentation_from_file(dataset_name)

    for key in ["notes", "methodology_other", "caveats"]:
        if documentation_dict[key] != "":
            attributes[key] = documentation_dict[key]

    return attributes


def read_documentation_from_file(dataset_name: str) -> dict:
    documentation = {"notes": "", "methodology_other": "", "caveats": ""}

    in_name_section = False

    documentation_file_path = ATTRIBUTES_FILEPATH.replace("attributes.csv", "documentation.md")
    with open(documentation_file_path, "r", encoding="UTF-8") as documentation_file:
        for text_line in documentation_file:
            if (
                text_line.startswith("##")
                and text_line.strip().endswith("##")
                and dataset_name in text_line
            ):
                in_name_section = True
                section_type = text_line.split(".")[1].replace("##", "").strip()
                continue
            if (
                text_line.startswith("##")
                and text_line.strip().endswith("##")
                and dataset_name not in text_line
            ):
                in_name_section = False
                continue

            if in_name_section:
                if section_type in documentation:
                    documentation[section_type] += text_line
                else:
                    documentation[section_type] = text_line

    return documentation


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
