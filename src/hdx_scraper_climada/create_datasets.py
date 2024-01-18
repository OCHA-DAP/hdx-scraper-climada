#!/usr/bin/env python
# encoding: utf-8

import datetime
import logging
import os
import time

from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

from hdx_scraper_climada.utilities import read_attributes, read_countries

setup_logging()
LOGGER = logging.getLogger(__name__)

try:
    Configuration.create(
        user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
        user_agent_lookup="hdx-scraper-climada",
    )
except ConfigurationError:
    LOGGER.info("Configuration already exists when trying to create in `create_datasets.py`")

INDICATOR_DIRECTORY = os.path.join(os.path.dirname(__file__), "output")


def create_datasets_in_hdx(
    dataset_name: str,
    dry_run: bool = False,
):
    LOGGER.info("*********************************************")
    LOGGER.info("* Climada - Create dataset   *")
    LOGGER.info(f"* Invoked at: {datetime.datetime.now().isoformat(): <23}    *")
    LOGGER.info("*********************************************")
    LOGGER.info(f"Dataset name: {dataset_name}")
    t0 = time.time()
    dataset_attributes = read_attributes(dataset_name)

    dataset, _ = create_or_fetch_base_dataset(dataset_name, dataset_attributes)

    countries_data = read_countries()
    countries_group = [{"name": x["iso3alpha_country_code"].lower()} for x in countries_data]

    LOGGER.info(f"Dataset title: {dataset['title']}")
    resource_names = dataset_attributes["resource"]

    # iso_date = datetime.datetime.now().isoformat()[0:10]
    # dataset["dataset_date"] = f"[{iso_date} to {iso_date}]".replace("Z", "")
    dataset["groups"] = countries_group

    LOGGER.info(f"Dataset date: {dataset['dataset_date']}")

    resource_list = []

    for resource_name in resource_names:
        attributes = read_attributes(resource_name)
        if "{country}" in resource_name:
            for country in countries_data:
                if country["iso3alpha_country_code"] in dataset_attributes["skip_country"]:
                    LOGGER.info("2024-01-10: Skipping Syria data whilst issue is addressed")
                    continue
                country_str = country["country_name"].lower().replace(" ", "-")
                resource_file_path = os.path.join(
                    INDICATOR_DIRECTORY,
                    dataset_attributes["output_subdirectory"],
                    attributes["filename_template"].format(country=country_str),
                )
                resource = Resource(
                    {
                        "name": os.path.basename(resource_file_path),
                        "description": attributes["description"].format(
                            country=country["country_name"]
                        ),
                        "format": attributes["file_format"],
                    }
                )
                resource.set_file_to_upload(resource_file_path)
                resource_list.append(resource)
        else:
            resource_file_path = os.path.join(
                INDICATOR_DIRECTORY,
                dataset_attributes["output_subdirectory"],
                attributes["filename_template"],
            )
            resource = Resource(
                {
                    "name": os.path.basename(resource_file_path),
                    "description": attributes["description"],
                    "format": attributes["file_format"],
                }
            )
            resource.set_file_to_upload(resource_file_path)
            resource_list.append(resource)

    dataset.add_update_resources(resource_list)
    if not dry_run:
        LOGGER.info("Dry_run flag not set so no data written to HDX")
        dataset.create_in_hdx()
    else:
        LOGGER.info("Dry_run flag set so no data written to HDX")
    LOGGER.info(f"Processing finished at {datetime.datetime.now().isoformat()}")
    LOGGER.info(f"Elapsed time: {time.time() - t0: 0.2f} seconds")

    return dataset


def create_or_fetch_base_dataset(
    dataset_name: str, dataset_attributes: dict, force_create: bool = False
) -> (Dataset, bool):
    dataset = Dataset.read_from_hdx(dataset_name)
    is_new = True
    if dataset is not None and not force_create:
        is_new = False
        LOGGER.info(f"Dataset already exists in hdx_site: `{Configuration.read().hdx_site}`")
        LOGGER.info("Updating")
    else:
        LOGGER.info(
            f"`{dataset_name}` does not exist in hdx_site: `{Configuration.read().hdx_site}`"
        )
        LOGGER.info(
            f"Using {dataset_attributes['dataset_template']} as a template for a new dataset"
        )
        dataset_template_filepath = os.path.join(
            os.path.dirname(__file__),
            "new-dataset-templates",
            dataset_attributes["dataset_template"],
        )

        dataset = Dataset.load_from_json(dataset_template_filepath)

        for attribute in ["title", "notes", "methodology_other", "caveats"]:
            dataset[attribute] = dataset_attributes[attribute]

    return dataset, is_new


if __name__ == "__main__":
    DATASET_NAME = "climada-litpop-dataset"
    create_datasets_in_hdx(DATASET_NAME)
