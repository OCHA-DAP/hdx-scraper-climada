#!/usr/bin/env python
# encoding: utf-8

"""
This test suite tests aspects of the climada interface for issue reporting 
Ian Hopkinson 2024-01-16
"""

from climada.entity import LitPop


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
