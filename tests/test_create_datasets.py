#!/usr/bin/env python
# encoding: utf-8

import json

from hdx_scraper_climada.create_datasets import create_or_fetch_base_dataset, create_datasets_in_hdx
from hdx_scraper_climada.utilities import read_attributes


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
