#!/usr/bin/env python
# encoding: utf-8

"""
This test suite tests aspects of the climada interface for issue reporting 
Ian Hopkinson 2024-01-16
"""

import os
import pandas as pd
import pytest


from climada.entity import LitPop
from hdx_scraper_climada.download_admin1_geometry import get_admin1_shapes_from_hdx
from hdx_scraper_climada.climada_interface import calculate_indicator_for_admin1
from hdx_scraper_climada.create_csv_files import make_detail_and_summary_file_paths

COUNTRY_ISO3A = "HTI"
COUNTRY = "Haiti"
ADMIN1_NAMES, ADMIN1_SHAPES = get_admin1_shapes_from_hdx(COUNTRY_ISO3A)


def test_afghanistan_litpop():
    country_iso3a = "AFG"
    afghanistan_litpop = LitPop.from_countries(country_iso3a)
    afghanistan_litpop_gdf = afghanistan_litpop.gdf

    assert not afghanistan_litpop_gdf["value"].isna().any()


def test_syria_litpop():
    country_iso3a = "SYR"
    syria_litpop = LitPop.from_countries(country_iso3a)
    syria_litpop_gdf = syria_litpop.gdf

    assert syria_litpop_gdf["value"].isna().sum() == len(syria_litpop_gdf)


def test_syria_litpop_nightlight_intensity():
    country_iso3a = "SYR"
    syria_litpop = LitPop.from_nightlight_intensity(country_iso3a)
    syria_litpop_gdf = syria_litpop.gdf

    assert syria_litpop_gdf["value"].isna().sum() == 0


def test_syria_litpop_population():
    country_iso3a = "SYR"
    syria_litpop = LitPop.from_population(country_iso3a)
    syria_litpop_gdf = syria_litpop.gdf

    assert syria_litpop_gdf["value"].isna().sum() == 0


def test_calculate_indicator_for_admin1_litpop():
    indicator = "litpop"

    admin1_indicator_gdf = calculate_indicator_for_admin1(
        ADMIN1_SHAPES[0], ADMIN1_NAMES[0], COUNTRY, indicator
    )

    print(admin1_indicator_gdf.columns, flush=True)
    print(admin1_indicator_gdf.iloc[0].to_dict(), flush=True)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "region_name": "Centre",
        "latitude": 19.3125,
        "longitude": -72.02083333,
        "aggregation": "none",
        "indicator": "litpop",
        "value": 759341.9415734566,
    }

    assert len(admin1_indicator_gdf) == 176


def test_calculate_indicator_for_admin1_litpop_alt():
    indicator = "litpop_alt"

    admin1_indicator_gdf = calculate_indicator_for_admin1(
        ADMIN1_SHAPES[0], ADMIN1_NAMES[0], COUNTRY, indicator
    )

    print(admin1_indicator_gdf.columns, flush=True)
    print(admin1_indicator_gdf.iloc[0].to_dict(), flush=True)

    # assert admin1_indicator_gdf.iloc[0].to_dict() == {
    #     "country_name": "Haiti",
    #     "region_name": "Centre",
    #     "latitude": 19.3125,
    #     "longitude": -72.02083333,
    #     "aggregation": "none",
    #     "indicator": "litpop",
    #     "value": 759341.9415734566,
    # }

    assert len(admin1_indicator_gdf) == 176


def test_litpop_cross_check():
    # This test shows that the litpop value from the Litpop Class and the litpop value from
    # the get_exposures method differ consistently by about 10%. This is likely an issue
    # with the GPW population statistics. It demonstrates that the region extraction code is
    # consistent between the two methods.
    admin1_litpop_gdf = calculate_indicator_for_admin1(
        ADMIN1_SHAPES[0], ADMIN1_NAMES[0], COUNTRY, "litpop"
    )
    admin1_litpop_alt_gdf = calculate_indicator_for_admin1(
        ADMIN1_SHAPES[0], ADMIN1_NAMES[0], COUNTRY, "litpop_alt"
    )

    assert len(admin1_litpop_gdf) == len(admin1_litpop_alt_gdf)

    for i in range(0, len(admin1_litpop_gdf)):
        litpop_row = admin1_litpop_gdf.iloc[i].to_dict()
        litpop_alt_row = admin1_litpop_alt_gdf.iloc[i].to_dict()
        assert litpop_row["country_name"] == "Haiti"
        assert litpop_alt_row["country_name"] == "Haiti"
        assert litpop_row["region_name"] == "Centre"
        assert litpop_alt_row["region_name"] == "Centre"
        assert litpop_row["indicator"] == "litpop"
        assert litpop_alt_row["indicator"] == "litpop_alt"
        assert litpop_row["latitude"] == litpop_alt_row["latitude"]
        assert litpop_row["longitude"] == litpop_alt_row["longitude"]

        assert litpop_row["value"] / litpop_alt_row["value"] == pytest.approx(0.890624986494022)

    # assert False


def test_calculate_indicator_for_admin1_crop_production():
    indicator = "crop_production"

    admin1_indicator_gdf_list = []
    for i, admin1_shape in enumerate(ADMIN1_SHAPES):
        admin1_indicator_gdf_list.append(
            calculate_indicator_for_admin1(admin1_shape, ADMIN1_NAMES[i], COUNTRY, indicator)
        )

    admin1_indicator_gdf = pd.concat(admin1_indicator_gdf_list)

    export_directory = os.path.join(os.path.dirname(__file__), "temp")
    detail_file_path, _ = make_detail_and_summary_file_paths(
        COUNTRY, indicator, export_directory=export_directory
    )
    admin1_indicator_gdf.to_csv(detail_file_path, index=False)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "region_name": "Centre",
        "latitude": 19.25,
        "longitude": -71.75,
        "aggregation": "none",
        "indicator": "crop_production.mai.noirr.USD",
        "value": 4550041.218882989,
    }

    assert len(admin1_indicator_gdf) == 104
