#!/usr/bin/env python
# encoding: utf-8

"""
This test suite tests aspects of the climada interface for issue reporting 
Ian Hopkinson 2024-01-16
"""
import pandas as pd

from climada.entity import LitPop
from hdx_scraper_climada.download_admin1_geometry import get_admin1_shapes_from_hdx
from hdx_scraper_climada.climada_interface import calculate_indicator_for_admin1

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


def test_calculate_indicator_for_admin1_crop_production():
    indicator = "crop_production"

    admin1_indicator_gdf_list = []
    for i, admin1_shape in enumerate(ADMIN1_SHAPES):
        admin1_indicator_gdf_list.append(
            calculate_indicator_for_admin1(admin1_shape, ADMIN1_NAMES[i], COUNTRY, indicator)
        )

    admin1_indicator_gdf = pd.concat(admin1_indicator_gdf_list)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "region_name": "Centre",
        "latitude": 19.25,
        "longitude": -71.75,
        "aggregation": "none",
        "indicator": "crop_production.mai.noirr.USD",
        "value": 4550041.218882989,
    }

    assert len(admin1_indicator_gdf) == 13
