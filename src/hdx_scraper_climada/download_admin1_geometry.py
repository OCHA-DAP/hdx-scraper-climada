#!/usr/bin/env python
# encoding: utf-8

import os
import logging

import geopandas
import shapely

from hdx.data.dataset import Dataset
from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration

ADMIN1_GEOMETRY_FOLDER = os.path.join(os.path.dirname(__file__), "admin1_geometry")

setup_logging()
LOGGER = logging.getLogger(__name__)

Configuration.create(hdx_site="prod", user_agent="hdxds_climada")


def download_hdx_admin1_boundaries():
    boundary_dataset = Dataset.read_from_hdx("unmap-international-boundaries-geojson")
    boundary_resources = boundary_dataset.get_resources()
    subn_resources = []
    for resource in boundary_resources:
        if "polbnda_adm" in resource["name"]:
            expected_file_path = os.path.join(ADMIN1_GEOMETRY_FOLDER, f"{resource['name']}")
            if os.path.exists(expected_file_path):
                print(f"Expected file {expected_file_path} is already present, continuing")
            print(f"Downloading {resource['name']}...", flush=True)
            resource_url, resource_file = resource.download(folder=ADMIN1_GEOMETRY_FOLDER)
            subn_resources.append(resource_file)
            print(f"...from {resource_url}", flush=True)


def get_admin1_shapes_from_hdx(country_iso3a):
    boundary_dataset = Dataset.read_from_hdx("unmap-international-boundaries-geojson")
    boundary_resources = boundary_dataset.get_resources()
    admin1_file_path = None
    for resource in boundary_resources:
        if "polbnda_adm1" in resource["name"]:
            admin1_file_path = os.path.join(ADMIN1_GEOMETRY_FOLDER, f"{resource['name']}")

    admin1_gpd = geopandas.read_file(admin1_file_path)

    admin1_for_country = admin1_gpd[admin1_gpd["alpha_3"] == country_iso3a.upper()]

    admin1_names = admin1_for_country["ADM1_REF"].to_list()

    # print(type(admin1_gpd), flush=True)
    # print(len(admin1_gpd), flush=True)

    # countries = set(admin1_gpd["alpha_3"].to_list())
    # print(len(countries), flush=True)
    # print(countries, flush=True)
    admin1_shapes = []
    for admin1_name in admin1_names:
        admin1_shape_geoseries = admin1_for_country[admin1_for_country["ADM1_REF"] == admin1_name][
            "geometry"
        ]
        admin1_shape_shapely = shapely.from_wkt(admin1_shape_geoseries.to_wkt())
        admin1_shapes.append(admin1_shape_geoseries)

    # print(admin1_names, flush=True)
    # print(admin1_gpd.keys(), flush=True)
    # admin1_info = admin1_info[country_iso3a]
    # admin1_shapes = admin1_shapes[country_iso3a]

    # admin1_names = [record["name"] for record in admin1_info]
    assert len(admin1_names) == len(admin1_shapes)
    return admin1_names, admin1_shapes


if __name__ == "__main__":
    # download_hdx_admin1_boundaries()
    ADMIN1_NAMES, ADMIN1_SHAPES = get_admin1_shapes_from_hdx("HTI")
    print(type(ADMIN1_SHAPES[0]), flush=True)
    print(ADMIN1_NAMES, flush=True)
