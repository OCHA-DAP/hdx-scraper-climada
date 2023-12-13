#!/usr/bin/env python
# encoding: utf-8

import os
import logging
import sys

import geopandas

from hdx.data.dataset import Dataset
from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration

ADMIN1_GEOMETRY_FOLDER = os.path.join(os.path.dirname(__file__), "admin1_geometry")
UNMAP_DATASET_NAME = "unmap-international-boundaries-geojson"

setup_logging()
LOGGER = logging.getLogger(__name__)

Configuration.create(hdx_site="prod", user_agent="hdxds_climada")


def download_hdx_admin1_boundaries():
    boundary_dataset = Dataset.read_from_hdx(UNMAP_DATASET_NAME)
    boundary_resources = boundary_dataset.get_resources()
    subn_resources = []
    for resource in boundary_resources:
        if "polbnda_adm1" in resource["name"]:
            expected_file_path = os.path.join(ADMIN1_GEOMETRY_FOLDER, f"{resource['name']}")
            if os.path.exists(expected_file_path):
                logging.info(f"Expected file {expected_file_path} is already present, continuing")
                subn_resources.append(expected_file_path)
            else:
                logging.info(f"Downloading {resource['name']}...")
                resource_url, resource_file = resource.download(folder=ADMIN1_GEOMETRY_FOLDER)

                logging.info(f"...from {resource_url}")
                subn_resources.append(resource_file)

    return subn_resources


def get_admin1_shapes_from_hdx(country_iso3a):
    boundary_dataset = Dataset.read_from_hdx(UNMAP_DATASET_NAME)
    boundary_resources = boundary_dataset.get_resources()
    admin1_file_path = None
    for resource in boundary_resources:
        if "polbnda_adm1" in resource["name"]:
            admin1_file_path = os.path.join(ADMIN1_GEOMETRY_FOLDER, f"{resource['name']}")

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
    return admin1_names, admin1_shapes


if __name__ == "__main__":
    COUNTRY_NAME = "HTI"
    if len(sys.argv) == 2:
        DATASET_NAME = sys.argv[1]
    RESOURCE_FILE_PATHS = download_hdx_admin1_boundaries()
    print(f"{len(RESOURCE_FILE_PATHS)} resources downloaded/present with file paths:", flush=True)
    for RESOURCE_FILE_PATH in RESOURCE_FILE_PATHS:
        print(RESOURCE_FILE_PATH, flush=True)
    ADMIN1_NAMES, ADMIN1_SHAPES = get_admin1_shapes_from_hdx("HTI")

    print(f"Admin1 boundaries for {COUNTRY_NAME} contain {len(ADMIN1_NAMES)} areas named:")
    for ADMIN1_NAME in ADMIN1_NAMES:
        print(ADMIN1_NAME, flush=True)
