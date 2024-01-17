#!/usr/bin/env python
# encoding: utf-8

import csv
import os
import pytest

from hdx_scraper_climada.create_csv_files import (
    create_detail_dataframes,
    create_summary_data,
    write_detail_data,
    write_summary_data,
    prepare_output_directory,
)

EXPORT_DIRECTORY = os.path.join(os.path.dirname(__file__), "temp")
COUNTRY = "Haiti"
INDICATOR = "litpop"


@pytest.fixture(scope="module")
def haiti_detail_dataframes():
    country_geodataframes_list = create_detail_dataframes(COUNTRY, INDICATOR, use_hdx_admin1=True)
    return country_geodataframes_list


def test_create_dataframes(haiti_detail_dataframes):
    assert haiti_detail_dataframes[0].columns.to_list() == [
        "country_name",
        "region_name",
        "latitude",
        "longitude",
        "aggregation",
        "indicator",
        "value",
    ]
    assert len(haiti_detail_dataframes) == 10


def test_create_summary(haiti_detail_dataframes):
    summary_rows, n_lines = create_summary_data(haiti_detail_dataframes, COUNTRY, INDICATOR)

    assert len(summary_rows) == 10
    assert n_lines == 1312


def test_write_detail_data(haiti_detail_dataframes):
    output_detail_path, _ = prepare_output_directory(
        COUNTRY, INDICATOR, export_directory=EXPORT_DIRECTORY
    )

    if os.path.exists(output_detail_path):
        os.remove(output_detail_path)

    status = write_detail_data(haiti_detail_dataframes, output_detail_path)

    print(status, flush=True)
    assert os.path.exists(output_detail_path)

    with open(output_detail_path, encoding="utf-8") as summary_file:
        rows = list(csv.DictReader(summary_file))

    assert len(rows) == 1313

    assert set(list(rows[0].keys())) == set(
        [
            "country_name",
            "region_name",
            "latitude",
            "longitude",
            "aggregation",
            "indicator",
            "value",
        ]
    )

    assert set(list(rows[0].values())) == set(
        ["#country", "#adm1+name", "#geo+lat", "#geo+lon", "", "#indicator+name", "#indicator+num"]
    )

    rows[1]["value"] = rows[1]["value"][0:11]
    assert set(list(rows[1].values())) == set(
        ["Haiti", "Centre", "19.3125", "-72.02083333", "none", "litpop", "759341.9415"]
    )


def test_write_summary_data(haiti_detail_dataframes):
    _, output_summary_path = prepare_output_directory(
        COUNTRY, INDICATOR, export_directory=EXPORT_DIRECTORY
    )

    if os.path.exists(output_summary_path):
        os.remove(output_summary_path)

    summary_rows, _ = create_summary_data(haiti_detail_dataframes, COUNTRY, INDICATOR)
    status = write_summary_data(summary_rows, output_summary_path)

    print(status, flush=True)

    assert os.path.exists(output_summary_path)

    with open(output_summary_path, encoding="utf-8") as summary_file:
        rows = list(csv.DictReader(summary_file))

    assert len(rows) == 11

    assert set(list(rows[0].keys())) == set(
        [
            "country_name",
            "region_name",
            "latitude",
            "longitude",
            "aggregation",
            "indicator",
            "value",
        ]
    )

    assert set(list(rows[0].values())) == set(
        ["#country", "#adm1+name", "#geo+lat", "#geo+lon", "", "#indicator+name", "#indicator+num"]
    )

    assert set(list(rows[1].values())) == set(
        ["Haiti", "Centre", "19.0069", "-71.9886", "sum", "litpop", "175640026.24312034"]
    )


def test_prepare_output_directory():
    _, _ = prepare_output_directory(COUNTRY, INDICATOR, export_directory=EXPORT_DIRECTORY)
    assert os.path.exists(os.path.join(EXPORT_DIRECTORY, f"{INDICATOR}"))
