#!/usr/bin/env python
# encoding: utf-8

import os
import logging

from hdx.data.dataset import Dataset
from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration

ADMIN1_GEOMETRY_FOLDER = os.path.join(os.path.dirname(__file__), "admin1_geometry")

setup_logging()
LOGGER = logging.getLogger(__name__)

Configuration.create(hdx_site="prod", user_agent="hdxds_climada", hdx_read_only=True)


def download_hdx_admin1_boundaries():
    boundary_dataset = Dataset.read_from_hdx("unmap-international-boundaries-geojson")
    boundary_resources = boundary_dataset.get_resources()
    subn_resources = []
    for resource in boundary_resources:
        if "polbnda_adm" in resource["name"]:
            expected_file_path = os.path.join(ADMIN1_GEOMETRY_FOLDER, f"{resource['name']}.geojson")
            if os.path.exists(expected_file_path):
                print(f"Expected file {expected_file_path} is already present, continuing")
            print(f"Downloading {resource['name']}...", flush=True)
            resource_url, resource_file = resource.download(folder=ADMIN1_GEOMETRY_FOLDER)
            subn_resources.append(resource_file)
            print(f"...from {resource_url}", flush=True)


if __name__ == "__main__":
    download_hdx_admin1_boundaries()
