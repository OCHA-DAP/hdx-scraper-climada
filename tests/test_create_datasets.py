#!/usr/bin/env python
# encoding: utf-8

import os
from hdx_scraper_climada.create_datasets import (
    create_or_fetch_base_dataset,
    create_datasets_in_hdx,
    compile_resource_list,
    compile_showcase_list,
    get_date_range_from_timeseries_file,
    make_countries_group,
)
from hdx_scraper_climada.utilities import read_attributes, read_countries, INDICATOR_LIST


def test_create_or_fetch_base_dataset():
    # This checks that UTF-8 is read from the dataset template
    dataset_name = "climada-litpop-dataset"
    dataset_attributes = read_attributes(dataset_name)
    dataset, is_new = create_or_fetch_base_dataset(
        dataset_name, dataset_attributes, force_create=True
    )

    assert dataset["title"] == (
        "LitPop: Humanitarian Response Plan (HRP) Countries "
        "Exposure Data for Disaster Risk Assessment"
    )
    assert is_new
    assert "Röösli" in dataset["methodology_other"]


def test_create_datasets_in_hdx():
    dataset_name = "climada-litpop-dataset"

    dataset = create_datasets_in_hdx(
        dataset_name,
        dry_run=True,
    )

    assert dataset["name"] == "climada-litpop-dataset"


def test_compile_resource_list():
    dataset_name = "climada-litpop-dataset"
    dataset_attributes = read_attributes(dataset_name)
    countries_data = read_countries(indicator="all")

    resource_list = compile_resource_list(dataset_attributes, countries_data, test_run=True)

    assert len(resource_list) == 23


def test_compile_showcase_list():
    dataset_name = "climada-flood-dataset"
    dataset_attributes = read_attributes(dataset_name)

    showcase_list = compile_showcase_list(dataset_attributes)

    assert len(showcase_list) == 1
    assert showcase_list[0]["name"] == "climada-flood-showcase"


def test_get_date_range_from_timeseries_file():
    # This fragment prints the
    # for indicator in INDICATOR_LIST:
    #     dataset_attributes = read_attributes(f"climada-{indicator}-dataset")
    #     date_range = get_date_range_from_timeseries_file(dataset_attributes)
    #     print(f"{indicator}: {date_range}", flush=True)
    # assert False

    output_directory = os.path.join(os.path.dirname(__file__), "fixtures")
    dataset_attributes = read_attributes("climada-flood-dataset")
    date_range = get_date_range_from_timeseries_file(
        dataset_attributes, output_directory=output_directory
    )

    assert date_range == "[2000-04-05T00:00:00 TO 2018-07-15T00:00:00]"


def test_make_countries_group_storm_europe():
    dataset_name = "climada-storm-europe-dataset"

    countries_group = make_countries_group(dataset_name)

    assert len(countries_group) == 1
    assert countries_group == [{"name": "ukr"}]


def test_make_countries_group_tropical_cyclone():
    dataset_name = "climada-tropical-cyclone-dataset"

    countries_group = make_countries_group(dataset_name)

    assert len(countries_group) == 8
    assert {x["name"] for x in countries_group} == set(
        ["col", "eth", "hti", "moz", "mmr", "som", "ven", "yem"]
    )
