#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_climada.create_csv_files import create_dataframes


def test_create_dataframes():
    country_litpop_gdf, country_geodataframes_list = create_dataframes(
        "Haiti", "litpop", use_hdx_admin1=True
    )

    assert country_litpop_gdf.columns.to_list() == [
        "country_name",
        "region_name",
        "latitude",
        "longitude",
        "aggregation",
        "indicator",
        "value",
    ]
    assert len(country_geodataframes_list) == 10
