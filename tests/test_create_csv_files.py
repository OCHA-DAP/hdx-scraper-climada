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

from hdx_scraper_climada.utilities import HAS_TIMESERIES

EXPORT_DIRECTORY = os.path.join(os.path.dirname(__file__), "temp")
COUNTRY = "Haiti"
INDICATOR = "earthquake"
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
    summary_rows, n_lines = create_summary_data(haiti_detail_dataframes)

    assert len(summary_rows) == 10
    assert n_lines == 1300


def test_write_detail_data(haiti_detail_dataframes):
    output_paths = make_detail_and_summary_file_paths(
        COUNTRY, INDICATOR, export_directory=EXPORT_DIRECTORY
    )

    if os.path.exists(output_paths["output_detail_path"]):
        os.remove(output_paths["output_detail_path"])

    _ = write_detail_data(haiti_detail_dataframes, output_paths["output_detail_path"])

    assert os.path.exists(output_paths["output_detail_path"])

    with open(output_paths["output_detail_path"], encoding="utf-8") as summary_file:
        rows = list(csv.DictReader(summary_file))

    assert len(rows) == 1301

    assert set(list(rows[0].keys())) == set(EXPECTED_COLUMN_LIST)

    assert set(list(rows[0].values())) == set(EXPECTED_HXL_TAGS)

    # The underlying litpop data has many decimal places, we round to an integer
    assert set(list(rows[1].values())) == set(
        ["Haiti", "Centre", "19.3125", "-72.02083333", "none", "litpop", "759342.0"]
    )


def test_write_summary_data(haiti_detail_dataframes):
    output_paths = make_detail_and_summary_file_paths(
        COUNTRY, INDICATOR, export_directory=EXPORT_DIRECTORY
    )

    if os.path.exists(output_paths["output_summary_path"]):
        os.remove(output_paths["output_summary_path"])

    summary_rows, _ = create_summary_data(haiti_detail_dataframes)
    _ = write_summary_data(summary_rows, output_paths["output_summary_path"])

    assert os.path.exists(output_paths["output_summary_path"])

    with open(output_paths["output_summary_path"], encoding="utf-8") as summary_file:
        rows = list(csv.DictReader(summary_file))

    assert len(rows) == 11

    assert set(list(rows[0].keys())) == set(EXPECTED_COLUMN_LIST)

    assert set(list(rows[0].values())) == set(EXPECTED_HXL_TAGS)

    assert set(list(rows[1].values())) == set(
        ["Haiti", "Centre", "19.0069", "-71.9886", "sum", "litpop", "175640022.0"]
    )


def test_make_detail_and_summary_file_paths():
    output_paths = make_detail_and_summary_file_paths(
        COUNTRY, INDICATOR, export_directory=EXPORT_DIRECTORY
    )

    assert "haiti-admin1-earthquake.csv" in output_paths["output_detail_path"]
    assert "admin1-summaries-earthquake.csv" in output_paths["output_summary_path"]
    assert "admin1-timeseries-summaries-earthquake.csv" in output_paths["output_timeseries_path"]

    for _, file_path in output_paths.items():
        assert os.path.dirname(file_path) == os.path.join(EXPORT_DIRECTORY, f"{INDICATOR}")


def test_export_indicator_data_to_csv_litpop():
    country = "Haiti"
    indicator = "crop-production"
    indicator_data_to_csv_helper(country, indicator)


def test_export_indicator_data_to_csv_crop_production():
    country = "Haiti"
    indicator = "crop-production"
    indicator_data_to_csv_helper(country, indicator)


def test_export_indicator_data_to_csv_earthquake():
    country = "Haiti"
    indicator = "earthquake"
    indicator_data_to_csv_helper(country, indicator)


def test_export_indicator_data_to_csv_flood():
    country = "Haiti"
    indicator = "flood"
    indicator_data_to_csv_helper(country, indicator)


def test_export_indicator_data_to_csv_wildfire():
    country = "Haiti"
    indicator = "wildfire"
    indicator_data_to_csv_helper(country, indicator)


def test_export_indicator_data_to_csv_river_flood():
    country = "Haiti"
    indicator = "river-flood"
    indicator_data_to_csv_helper(country, indicator)


def test_export_indicator_data_to_csv_tropical_cyclone():
    country = "Haiti"
    indicator = "tropical-cyclone"
    indicator_data_to_csv_helper(country, indicator)


@pytest.mark.skip(reason="Runtime over 10 minutes - currently failing")
def test_export_indicator_data_to_csv_storm_europe():
    country = "Ukraine"
    indicator = "storm-europe"
    indicator_data_to_csv_helper(country, indicator)


def indicator_data_to_csv_helper(country: str, indicator: str):
    output_paths = make_detail_and_summary_file_paths(
        country, indicator, export_directory=EXPORT_DIRECTORY
    )
    if os.path.exists(output_paths["output_detail_path"]):
        os.remove(output_paths["output_detail_path"])

    if os.path.exists(output_paths["output_summary_path"]):
        os.remove(output_paths["output_summary_path"])

    if indicator in HAS_TIMESERIES:
        if os.path.exists(output_paths["output_timeseries_path"]):
            os.remove(output_paths["output_timeseries_path"])

    statuses = export_indicator_data_to_csv(country, indicator, export_directory=EXPORT_DIRECTORY)

    for status in statuses:
        print(status, flush=True)

    if indicator in HAS_TIMESERIES:
        assert len(statuses) == 4
    else:
        assert len(statuses) == 3
    assert os.path.exists(output_paths["output_summary_path"])
    assert os.path.exists(output_paths["output_detail_path"])
    if indicator in HAS_TIMESERIES:
        assert os.path.exists(output_paths["output_timeseries_path"])

    with open(output_paths["output_summary_path"], encoding="utf-8") as summary_file:
        rows = list(csv.DictReader(summary_file))

    if indicator == "crop-production":
        assert len(rows) == 81  # 10 regions x 8 indicators + 1 HXL tag
    else:
        assert len(rows) == 11  # 10 regions + 1 HXL tag
