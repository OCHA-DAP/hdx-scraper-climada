#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_climada.create_csv_files import create_dataframes, create_summary_data


def test_create_dataframes():
    country_geodataframes_list = create_dataframes("Haiti", "litpop", use_hdx_admin1=True)

    assert country_geodataframes_list[0].columns.to_list() == [
        "country_name",
        "region_name",
        "latitude",
        "longitude",
        "aggregation",
        "indicator",
        "value",
    ]
    assert len(country_geodataframes_list) == 10


def test_create_summary():
    country = "Haiti"
    indicator = "litpop"
    country_geodataframes_list = create_dataframes("Haiti", "litpop", use_hdx_admin1=True)
    summary_rows = create_summary_data(country_geodataframes_list, country, indicator)

    for row in summary_rows:
        print(row, flush=True)

    assert len(summary_rows) == 10
