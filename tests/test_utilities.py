#!/usr/bin/env python
# encoding: utf-8

import csv
import os
from pathlib import Path
from hdx_scraper_climada.utilities import read_attributes, write_dictionary

TEMP_FILE_PATH = os.path.join(Path(__file__).parent, "temp", "tmp.csv")
DICT_LIST = [
    {"a": 1, "b": 2, "c": 3},
    {"a": 4, "b": 5, "c": 6},
    {"a": 7, "b": 8, "c": 9},
]


def test_read_attributes():
    dataset_name = "climada-litpop-dataset"
    dataset_attributes = read_attributes(dataset_name)

    assert len(dataset_attributes["resource"]) == 2
    assert dataset_attributes["skip_country"] == ["SYR"]
    assert dataset_attributes["data_type_group"] == "hazard"


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
