#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_climada.download_admin1_geometry import (
    download_hdx_admin1_boundaries,
    get_admin1_shapes_from_hdx,
)


def test_download_hdx_admin1_boundaries():
    local_resource_paths = download_hdx_admin1_boundaries()

    assert len(local_resource_paths) == 1
    assert "polbnda_adm1_1m_ocha.geojson" in local_resource_paths[0]
    assert "admin1_geometry" in local_resource_paths[0]


def test_get_admin1_shapes_from_hdx():
    country_isoa3 = "HTI"

    admin1_names, admin1_shapes = get_admin1_shapes_from_hdx(country_isoa3)

    assert set(admin1_names) == set(
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

    assert len(admin1_shapes) == 10


def test_get_admin1_shapes_from_hdx_no_data_case():
    country_isoa3 = "GBR"

    admin1_names, admin1_shapes = get_admin1_shapes_from_hdx(country_isoa3)

    assert admin1_names is None
    assert admin1_shapes is None
