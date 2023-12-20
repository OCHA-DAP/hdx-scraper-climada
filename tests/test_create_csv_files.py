#!/usr/bin/env python
# encoding: utf-8

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

    print(syria_litpop_gdf.columns, flush=True)
    print(syria_litpop_gdf, flush=True)
    print(syria_litpop_gdf["value"].isna().sum(), flush=True)

    assert syria_litpop_gdf["value"].isna().sum() == len(syria_litpop_gdf)
