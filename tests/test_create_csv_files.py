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
    make_detail_and_summary_file_paths,
    export_indicator_data_to_csv,
)

EXPORT_DIRECTORY = os.path.join(os.path.dirname(__file__), "temp")
COUNTRY = "Haiti"
INDICATOR = "litpop"
EXPECTED_COLUMN_LIST = [
    "country_name",
    "region_name",
    "latitude",
    "longitude",
    "aggregation",
    "indicator",
    "value",
]

EXPECTED_HXL_TAGS = [
    "#country",
    "#adm1+name",
    "#geo+lat",
    "#geo+lon",
    "",
    "#indicator+name",
    "#indicator+num",
]


@pytest.fixture(scope="module")
def haiti_detail_dataframes():
    country_geodataframes_list = create_detail_dataframes(COUNTRY, INDICATOR, use_hdx_admin1=True)
    return country_geodataframes_list


def test_create_dataframes(haiti_detail_dataframes):
    assert haiti_detail_dataframes[0].columns.to_list() == EXPECTED_COLUMN_LIST
    assert len(haiti_detail_dataframes) == 10


def test_create_summary(haiti_detail_dataframes):
    summary_rows, n_lines = create_summary_data(haiti_detail_dataframes, COUNTRY, INDICATOR)

    assert len(summary_rows) == 10
    assert n_lines == 1312


def test_write_detail_data(haiti_detail_dataframes):
    output_detail_path, _ = make_detail_and_summary_file_paths(
        COUNTRY, INDICATOR, export_directory=EXPORT_DIRECTORY
    )

    if os.path.exists(output_detail_path):
        os.remove(output_detail_path)

    _ = write_detail_data(haiti_detail_dataframes, output_detail_path)

    assert os.path.exists(output_detail_path)

    with open(output_detail_path, encoding="utf-8") as summary_file:
        rows = list(csv.DictReader(summary_file))

    assert len(rows) == 1313

    assert set(list(rows[0].keys())) == set(EXPECTED_COLUMN_LIST)

    assert set(list(rows[0].values())) == set(EXPECTED_HXL_TAGS)

    # We do this because the trailing digits are unstable - they are in the +10 sig figs
    # so no practical concern. Probably a result of floating point errors
    rows[1]["value"] = rows[1]["value"][0:11]
    assert set(list(rows[1].values())) == set(
        ["Haiti", "Centre", "19.3125", "-72.02083333", "none", "litpop", "759341.9415"]
    )


def test_write_summary_data(haiti_detail_dataframes):
    _, output_summary_path = make_detail_and_summary_file_paths(
        COUNTRY, INDICATOR, export_directory=EXPORT_DIRECTORY
    )

    if os.path.exists(output_summary_path):
        os.remove(output_summary_path)

    summary_rows, _ = create_summary_data(haiti_detail_dataframes, COUNTRY, INDICATOR)
    _ = write_summary_data(summary_rows, output_summary_path)

    assert os.path.exists(output_summary_path)

    with open(output_summary_path, encoding="utf-8") as summary_file:
        rows = list(csv.DictReader(summary_file))

    assert len(rows) == 11

    assert set(list(rows[0].keys())) == set(EXPECTED_COLUMN_LIST)

    assert set(list(rows[0].values())) == set(EXPECTED_HXL_TAGS)

    assert set(list(rows[1].values())) == set(
        ["Haiti", "Centre", "19.0069", "-71.9886", "sum", "litpop", "175640026.24312034"]
    )


def test_make_detail_and_summary_file_paths():
    output_detail_path, output_summary_path = make_detail_and_summary_file_paths(
        COUNTRY, INDICATOR, export_directory=EXPORT_DIRECTORY
    )

    assert "haiti-admin1-litpop.csv" in output_detail_path
    assert "admin1-summaries-litpop.csv" in output_summary_path

    assert os.path.exists(os.path.join(EXPORT_DIRECTORY, f"{INDICATOR}"))


def test_export_indicator_data_to_csv():
    country = "Haiti"
    indicator = "crop_production"
    output_detail_path, output_summary_path = make_detail_and_summary_file_paths(
        country, indicator, export_directory=EXPORT_DIRECTORY
    )
    if os.path.exists(output_detail_path):
        os.remove(output_detail_path)

    if os.path.exists(output_summary_path):
        os.remove(output_summary_path)

    statuses = export_indicator_data_to_csv(country, indicator, export_directory=EXPORT_DIRECTORY)

    for status in statuses:
        print(status, flush=True)

    assert len(statuses) == 3
    assert os.path.exists(output_detail_path)
    assert os.path.exists(output_summary_path)
