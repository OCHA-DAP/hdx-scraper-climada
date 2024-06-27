#!/usr/bin/env python
# encoding: utf-8

import logging
import os

from hdx_scraper_climada.download_from_hdx import (
    download_hdx_admin1_boundaries,
    get_admin1_shapes_from_hdx,
    get_admin2_shapes_from_hdx,
    get_best_admin_shapes,
)


def test_download_hdx_admin1_boundaries():
    local_resource_paths = download_hdx_admin1_boundaries()

    assert len(local_resource_paths) == 2
    filenames = {os.path.basename(x) for x in local_resource_paths}
    assert "polbnda_adm1_1m_ocha.geojson" in filenames
    assert "polbnda_adm2_1m_ocha.geojson" in filenames
    for path_ in local_resource_paths:
        assert "admin1_geometry" in path_


HTI_ADMIN1_NAMES = set(
    [
        "Centre",
        "North-West",
        "South-East",
        "South",
        "Nippes",
        "Grande'Anse",
        "West",
        "North",
        "North-East",
        "Artibonite",
    ]
)


def test_get_admin1_shapes_from_hdx():
    country_isoa3 = "HTI"

    admin1_names, admin1_shapes = get_admin1_shapes_from_hdx(country_isoa3)

    assert set(admin1_names) == HTI_ADMIN1_NAMES

    assert len(admin1_shapes) == 10


def test_get_admin1_shapes_from_hdx_no_data_case(caplog):
    caplog.set_level(logging.INFO)
    country_isoa3 = "GBR"

    admin1_names, admin1_shapes = get_admin1_shapes_from_hdx(country_isoa3)

    assert len(admin1_names) == 4
    assert len(admin1_shapes) == 4
    assert "UNMAP data not found for GBR, trying GeoBoundaries" in caplog.text


def test_get_admin2_shapes_from_hdx():
    country_isoa3 = "HTI"

    admin1_names, admin2_names, admin_shapes = get_admin2_shapes_from_hdx(country_isoa3)

    assert set(admin1_names) == HTI_ADMIN1_NAMES
    assert len(admin_shapes) == 140
    assert len(admin2_names) == 140


def test_get_best_admin_shapes():
    country_isoa3 = "HTI"
    _, _, _, admin_level = get_best_admin_shapes(country_isoa3)
    assert admin_level == "2"
