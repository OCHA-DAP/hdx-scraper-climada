#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_climada.utilities import read_attributes


def test_read_attributes():
    dataset_name = "climada-litpop-dataset"
    dataset_attributes = read_attributes(dataset_name)

    assert len(dataset_attributes["resource"]) == 2
    assert dataset_attributes["skip_country"] == ["SYR"]
    assert dataset_attributes["data_type_group"] == "hazard"
