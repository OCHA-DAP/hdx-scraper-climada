#!/usr/bin/env python
# encoding: utf-8

import fnmatch
import os
import logging

from pathlib import Path

import geopandas

import climada.util.coordinates as u_coord

from hdx.data.dataset import Dataset
from hdx.utilities.easy_logging import setup_logging
from hdx_scraper_climada.create_datasets import configure_hdx_connection

ADMIN1_GEOMETRY_FOLDER = os.path.join(os.path.dirname(__file__), "admin1_geometry")
UNMAP_DATASET_NAME = "unmap-international-boundaries-geojson"

setup_logging()
LOGGER = logging.getLogger(__name__)


def download_hdx_admin1_boundaries(
    download_directory: str = None, hdx_site: str = "stage"
) -> list[str]:
    configure_hdx_connection(hdx_site=hdx_site)
    if download_directory is None:
        download_directory = ADMIN1_GEOMETRY_FOLDER
    else:
        download_directory = os.path.join(os.getcwd(), download_directory)
        Path(download_directory).mkdir(parents=True, exist_ok=True)

    boundary_dataset = Dataset.read_from_hdx(UNMAP_DATASET_NAME)
    boundary_resources = boundary_dataset.get_resources()
    subn_resources = []
    for resource in boundary_resources:
        if "polbnda_adm1" in resource["name"] or "polbnda_adm2" in resource["name"]:
            expected_file_path = os.path.join(download_directory, f"{resource['name']}")
            if os.path.exists(expected_file_path):
                LOGGER.info(f"Expected file {expected_file_path} is already present, continuing")
                subn_resources.append(expected_file_path)
            else:
                LOGGER.info(f"Downloading {resource['name']}...")
                resource_url, resource_file = resource.download(folder=download_directory)

                LOGGER.info(f"...from {resource_url}")
                subn_resources.append(resource_file)

    return subn_resources


def get_admin1_shapes_from_hdx(country_iso3a):
    admin1_file_path = os.path.join(ADMIN1_GEOMETRY_FOLDER, "polbnda_adm1_1m_ocha.geojson")

    if not os.path.exists(admin1_file_path):
        raise FileNotFoundError(
            f"{admin1_file_path} was not found, run `download_admin1_geometry.py` to download"
        )

    admin1_gpd = geopandas.read_file(admin1_file_path)

    admin1_for_country = admin1_gpd[admin1_gpd["alpha_3"] == country_iso3a.upper()]

    admin1_names = admin1_for_country["ADM1_REF"].to_list()

    admin1_shapes = []
    for admin1_name in admin1_names:
        admin1_shape_geoseries = admin1_for_country[admin1_for_country["ADM1_REF"] == admin1_name][
            "geometry"
        ]
        admin1_shapes.append(admin1_shape_geoseries)

    assert len(admin1_names) == len(admin1_shapes)

    if len(admin1_shapes) == 0:
        LOGGER.info(f"UNMAP data not found for {country_iso3a}, trying Natural Earth")
        admin1_names, admin1_shapes = get_admin1_shapes_from_natural_earth(country_iso3a)
    return admin1_names, admin1_shapes


def get_admin2_shapes_from_hdx(
    country_iso3a: str,
) -> tuple[list, list, list[geopandas.GeoDataFrame]]:
    admin2_file_path = os.path.join(ADMIN1_GEOMETRY_FOLDER, "polbnda_adm2_1m_ocha.geojson")

    if not os.path.exists(admin2_file_path):
        raise FileNotFoundError(
            f"{admin2_file_path} was not found, run `download_admin1_geometry.py` to download"
        )

    admin2_gpd = geopandas.read_file(admin2_file_path)

    admin2_for_country = admin2_gpd[admin2_gpd["alpha_3"] == country_iso3a.upper()]

    admin1_names = admin2_for_country["ADM1_REF"].to_list()
    admin2_names = admin2_for_country["ADM2_REF"].to_list()

    admin2_shapes = []
    for admin2_name in admin2_names:
        admin2_shape_geoseries = admin2_for_country[admin2_for_country["ADM2_REF"] == admin2_name][
            "geometry"
        ]
        admin2_shapes.append(admin2_shape_geoseries)

    assert len(admin2_names) == len(admin2_shapes)
    return admin1_names, admin2_names, admin2_shapes


def get_best_admin_shapes(
    country_iso3alpha: str,
) -> tuple[list, list, list[geopandas.GeoDataFrame], str]:
    admin_level = "2"
    admin1_names, admin2_names, admin_shapes = get_admin2_shapes_from_hdx(country_iso3alpha)
    if len(admin2_names) == 0:
        admin1_names, admin_shapes = get_admin1_shapes_from_hdx(country_iso3alpha)
        admin_level = "1"
        admin2_names = len(admin1_names) * [""]

    return admin1_names, admin2_names, admin_shapes, admin_level


def download_hdx_datasets(
    dataset_filter: str,
    resource_filter: str = "*",
    hdx_site: str = "stage",
    download_directory: str = None,
):
    configure_hdx_connection(hdx_site=hdx_site)
    if download_directory is None:
        download_directory = os.path.join(os.path.dirname(__file__), "output")

    Path(download_directory).mkdir(parents=True, exist_ok=True)

    if resource_filter is None:
        resource_filter = "*"

    dataset = Dataset.read_from_hdx(dataset_filter)
    resources = dataset.get_resources()
    download_paths = []
    for resource in resources:
        if fnmatch.fnmatch(resource["name"], resource_filter):
            expected_file_path = os.path.join(download_directory, f"{resource['name']}")
            if os.path.exists(expected_file_path):
                LOGGER.info(f"Expected file {expected_file_path} is already present, continuing")
                download_paths.append(expected_file_path)
            else:
                LOGGER.info(f"Downloading {resource['name']}...")
                resource_url, resource_file = resource.download(folder=download_directory)
                LOGGER.info(f"...from {resource_url}")
                download_paths.append(resource_file)

    return download_paths


def get_admin1_shapes_from_natural_earth(country_iso3a):
    try:
        admin1_info, admin1_shapes = u_coord.get_admin1_info(country_iso3a)
        admin1_info = admin1_info[country_iso3a]
        admin1_shapes = admin1_shapes[country_iso3a]

        admin1_names = [record["name"] for record in admin1_info]
    except LookupError as error:
        LOGGER.info(error)
        admin1_names = []
        admin1_shapes = []

    return admin1_names, admin1_shapes


if __name__ == "__main__":
    RESOURCE_FILE_PATHS = download_hdx_admin1_boundaries()
    LOGGER.info(f"Downloaded admin1 boundary data to: {RESOURCE_FILE_PATHS}")
