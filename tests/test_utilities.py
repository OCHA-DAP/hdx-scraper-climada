#!/usr/bin/env python
# encoding: utf-8

import csv
import logging
import os
from pathlib import Path

import pytest

from hdx_scraper_climada.utilities import (
    read_attributes,
    write_dictionary,
    print_banner_to_log,
    read_documentation_from_file,
    read_countries,
)

TEMP_FILE_PATH = os.path.join(Path(__file__).parent, "temp", "tmp.csv")
DICT_LIST = [
    {"a": 1, "b": 2, "c": 3},
    {"a": 4, "b": 5, "c": 6},
    {"a": 7, "b": 8, "c": 9},
]

LOGGER = logging.getLogger(__name__)


def test_read_attributes():
    dataset_name = "climada-litpop-dataset"
    dataset_attributes = read_attributes(dataset_name)

    assert len(dataset_attributes["resource"]) == 2
    assert dataset_attributes["skip_country"] == ["SYR"]
    assert dataset_attributes["data_type_group"] == "exposure"
    assert dataset_attributes["notes"].startswith("A high-resolution asset exposure")
    assert dataset_attributes["methodology_other"].startswith("Created from the Climada API")
    assert dataset_attributes["caveats"].startswith(
        "2024-01-10: There is an issue with the Syria data so it is not included, "
        "this issue is being addressed"
    )


def test_read_documentation_from_file():
    dataset_name = "climada-litpop-dataset"
    documentation_dict = read_documentation_from_file(dataset_name)

    assert set(documentation_dict.keys()) == set(["notes", "methodology_other", "caveats"])
    assert documentation_dict["notes"].startswith("A high-resolution asset exposure")
    assert documentation_dict["methodology_other"].startswith("Created from the Climada API")
    assert documentation_dict["caveats"].startswith(
        "2024-01-10: There is an issue with the Syria data so it is not included, "
        "this issue is being addressed"
    )


def test_write_dictionary_to_local_file():
    if os.path.isfile(TEMP_FILE_PATH):
        os.remove(TEMP_FILE_PATH)

    status = write_dictionary(TEMP_FILE_PATH, DICT_LIST)
    with open(TEMP_FILE_PATH, encoding="utf-8") as temp_file:
        rows_read = list(csv.DictReader(temp_file))

    assert len(rows_read) == 3
    assert rows_read[0] == {"a": "1", "b": "2", "c": "3"}
    assert "New file" in status
    assert "is being created" in status


@pytest.mark.skip(
    reason="This test does not work in VS Code because "
    "we supress logging with '-p no:logging' in settings.json"
)
def test_print_banner_to_log(caplog):
    caplog.set_level(logging.INFO)
    print_banner_to_log(LOGGER, "test-banner")

    log_rows = caplog.text.split("\n")
    assert len(log_rows) == 5
    assert len(log_rows[0]) == len(log_rows[1])
    assert "test-banner" in caplog.text


def test_read_countries_all():
    countries = read_countries()
    assert len(countries) == 49
    assert len(countries) == len({x["country_name"] for x in countries})


def test_read_countries_earthquake():
    countries = read_countries(indicator="earthquake")
    assert len(countries) == 35
    assert len(countries) == len({x["iso3alpha_country_code"] for x in countries})


def test_read_countries_tropical_cyclone():
    countries = read_countries(indicator="tropical-cyclone")
    assert len(countries) == 36
    assert len(countries) == len({x["iso3alpha_country_code"] for x in countries})


def test_read_countries_floods():
    countries = read_countries(indicator="floods")
    assert len(countries) == 46
    assert len(countries) == len({x["iso3alpha_country_code"] for x in countries})
