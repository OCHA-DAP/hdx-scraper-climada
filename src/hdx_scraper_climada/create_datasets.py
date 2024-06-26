#!/usr/bin/env python
# encoding: utf-8

import datetime
import json
import logging
import os
import time
import traceback

import pandas
import yaml

from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase

from hdx_scraper_climada.utilities import read_attributes, read_countries, NO_DATA

setup_logging()
LOGGER = logging.getLogger(__name__)

INDICATOR_DIRECTORY = os.path.join(os.path.dirname(__file__), "output")


def create_datasets_in_hdx(
    dataset_name: str, dry_run: bool = True, hdx_site: str = "stage", force_create: bool = True
):
    LOGGER.info("*********************************************")
    LOGGER.info("* Climada - Create dataset   *")
    LOGGER.info(f"* Invoked at: {datetime.datetime.now().isoformat(): <23}    *")
    LOGGER.info("*********************************************")
    LOGGER.info(f"Dataset name: {dataset_name}")
    t0 = time.time()
    configure_hdx_connection(hdx_site=hdx_site)
    dataset_attributes = read_attributes(dataset_name)
    # A bit nasty, we don't store the indicator in the dataset attributes but we use the
    # indicator as the name of the output_subdirectory
    indicator = dataset_attributes["output_subdirectory"]
    countries_data = read_countries(indicator=indicator)

    # Get dataset_date from HDX
    file_dataset_date = get_date_range_from_timeseries_file(dataset_attributes)
    # Get dataset_date from
    hdx_dataset_date = get_date_range_from_hdx(None, hdx_site=hdx_site, dataset_name=dataset_name)

    LOGGER.info(f"dataset_date from new build: {file_dataset_date}")
    LOGGER.info(f"dataset_date from HDX: {hdx_dataset_date}")

    # In GitHub Actions environment variables are transmitted between job steps via the file
    # at $GITHUB_ENV but within a job step they sholud be accessed as normal environment variables.
    # However, this seems not to work.
    if os.getenv("GITHUB_ENV"):
        if file_dataset_date == hdx_dataset_date:
            LOGGER.info("No new data by dataset_date - but updating production")
            with open(os.getenv("GITHUB_ENV"), "a", encoding="utf-8") as environment_file:
                environment_file.write("CLIMADA_NEW_DATA=No")
            # return None
        else:
            with open(os.getenv("GITHUB_ENV"), "a", encoding="utf-8") as environment_file:
                environment_file.write("CLIMADA_NEW_DATA=Yes")

    dataset, _ = create_or_fetch_base_dataset(
        dataset_name, dataset_attributes, hdx_site=hdx_site, force_create=force_create
    )

    dataset["dataset_date"] = file_dataset_date
    dataset["groups"] = make_countries_group(dataset_name)
    dataset["maintainer"] = "76f545b9-6944-41c8-a999-eeb1bb70de7a"  # this is Emanuel
    # dataset["maintainer"] = "972627a5-4f23-4922-8892-371ece6531b6"  # this is me

    LOGGER.info(f"Dataset date: {dataset['dataset_date']}")

    # Compile resources
    resource_list = compile_resource_list(dataset_attributes, countries_data)
    dataset.add_update_resources(resource_list)

    # Compile showcases
    showcase_list = compile_showcase_list(dataset_attributes)

    # Add in a quickchart
    dataset, quickchart_status = add_quickchart(dataset, dataset_attributes)

    LOGGER.info(f"Dataset title: {dataset['title']}")
    LOGGER.info(f"{len(resource_list)} resources and {len(showcase_list)} showcases")
    LOGGER.info(f"{quickchart_status}")
    if not dry_run:
        LOGGER.info("Dry_run flag not set so data is being written to HDX")
        dataset.create_in_hdx()
        for showcase in showcase_list:
            showcase.create_in_hdx()
            showcase.add_dataset(dataset)
    else:
        LOGGER.info("Dry_run flag set so no data written to HDX")
    LOGGER.info(f"Processing finished at {datetime.datetime.now().isoformat()}")
    LOGGER.info(f"Elapsed time: {time.time() - t0: 0.2f} seconds")

    return dataset


def make_countries_group(dataset_name: str) -> list[dict]:
    countries_group = []

    indicator = dataset_name.replace("climada-", "").replace("-dataset", "")
    countries_data = read_countries(indicator=indicator)

    if indicator == "storm-europe":
        countries_group = [{"name": "ukr"}]
    else:
        countries_group = [
            {"name": x["iso3alpha_country_code"].lower()}
            for x in countries_data
            if x["country_name"] not in NO_DATA[indicator]
        ]

    return countries_group


def configure_hdx_connection(hdx_site: str = "stage"):
    try:
        Configuration.delete()
        Configuration.create(
            user_agent_config_yaml=os.path.join(os.path.expanduser("~"), ".useragents.yaml"),
            user_agent_lookup="hdx-scraper-climada",
            hdx_site=hdx_site,
        )
        LOGGER.info(f"Authenticated to HDX site at {Configuration.read().get_hdx_site_url()}")
        hdx_key = Configuration.read().get_api_key()
        LOGGER.info(f"With HDX_KEY ending {hdx_key[-10:]}")

    except ConfigurationError:
        LOGGER.info(traceback.format_exc())
        LOGGER.info("Configuration already exists when trying to create in `create_datasets.py`")


def compile_resource_list(
    dataset_attributes: dict, countries_data: list[dict], test_run: bool = False
) -> list[dict]:
    resource_names = dataset_attributes["resource"]
    resource_list = []
    for resource_name in resource_names:
        attributes = read_attributes(resource_name)
        if "{country}" in resource_name:
            for country in countries_data:
                if country["iso3alpha_country_code"] in dataset_attributes.get("skip_country", []):
                    LOGGER.info("2024-01-10: Skipping Syria data whilst issue is addressed")
                    continue
                country_str = country["country_name"].lower().replace(" ", "-")
                resource_file_path = os.path.join(
                    INDICATOR_DIRECTORY,
                    dataset_attributes["output_subdirectory"],
                    attributes["filename_template"].format(country=country_str),
                )
                if os.path.exists(resource_file_path) or test_run:
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
                    LOGGER.info(f"Detail file for {country['country_name']} does not exist")
        else:
            resource_file_path = os.path.join(
                INDICATOR_DIRECTORY,
                dataset_attributes["output_subdirectory"],
                attributes["filename_template"],
            )
            if os.path.exists(resource_file_path) or test_run:
                resource = Resource(
                    {
                        "name": os.path.basename(resource_file_path),
                        "description": attributes["description"],
                        "format": attributes["file_format"],
                    }
                )
                resource.set_file_to_upload(resource_file_path)
                resource_list.append(resource)
            else:
                LOGGER.info(f"Resource file path {resource_file_path} does not exist")

    return resource_list


def compile_showcase_list(dataset_attributes: dict):
    showcase_list = []

    showcase_names = dataset_attributes.get("showcase", [])

    for showcase_name in showcase_names:
        showcase_attributes = read_attributes(showcase_name)
        showcase = Showcase(
            {
                "name": showcase_attributes["name"],
                "title": showcase_attributes["title"],
                "notes": showcase_attributes["notes"],
                "url": showcase_attributes["url"],
                "image_url": showcase_attributes["image_url"],
            }
        )
        added_tags, unadded_tags = showcase.add_tags(showcase_attributes["tags"])
        LOGGER.info(f"{len(added_tags)} of {len(showcase_attributes['tags'])} showcase tags added")
        if len(unadded_tags) != 0:
            LOGGER.info(f"Rejected showcase tags: {unadded_tags}")
        showcase_list.append(showcase)

    return showcase_list


def create_or_fetch_base_dataset(
    dataset_name: str, dataset_attributes: dict, hdx_site: str = "stage", force_create: bool = False
) -> tuple[Dataset, bool]:
    configure_hdx_connection(hdx_site=hdx_site)
    dataset = Dataset.read_from_hdx(dataset_name)
    is_new = True
    if dataset is not None and not force_create:
        is_new = False
        LOGGER.info(f"Dataset already exists in hdx_site: `{Configuration.read().hdx_site}`")
        LOGGER.info("Updating")
    else:
        if not force_create:
            LOGGER.info(
                f"`{dataset_name}` does not exist in hdx_site: `{Configuration.read().hdx_site}`"
            )
        else:
            LOGGER.info("Force_create is set")
        LOGGER.info(
            f"Using {dataset_attributes['dataset_template']} as a template for a new dataset"
        )
        dataset_template_filepath = os.path.join(
            os.path.dirname(__file__),
            "new-dataset-templates",
            dataset_attributes["dataset_template"],
        )

        dataset = Dataset.load_from_json(dataset_template_filepath)

        dataset["name"] = dataset_name
        for attribute in ["title", "notes", "methodology_other", "caveats"]:
            dataset[attribute] = dataset_attributes[attribute]

    return dataset, is_new


def add_quickchart(dataset: Dataset, dataset_attributes: dict) -> tuple[str, Dataset]:
    status = ""

    resource_name = dataset_attributes.get("quickchart_resource_name", None)
    hdx_hxl_preview_file_path = dataset_attributes.get("quickchart_json_file_path", None)

    if hdx_hxl_preview_file_path is not None:
        hdx_hxl_preview_file_path = os.path.join(
            os.path.dirname(__file__), hdx_hxl_preview_file_path
        )

    if resource_name is None or hdx_hxl_preview_file_path is None:
        status = "No information provided for a Quick Chart"
        return None, status

    with open(hdx_hxl_preview_file_path, "r", encoding="utf-8") as json_file:
        recipe = json.load(json_file)
    # extract appropriate keys
    processed_recipe = {
        "description": "",
        "title": "Quick Charts",
        "view_type": "hdx_hxl_preview",
        "hxl_preview_config": "",
    }

    # convert the configuration to a string
    stringified_config = json.dumps(
        recipe["hxl_preview_config"], indent=None, separators=(",", ":")
    )
    processed_recipe["hxl_preview_config"] = stringified_config
    # write out yaml to a temp file
    temp_yaml_path = f"{hdx_hxl_preview_file_path}.temp.yaml"
    with open(temp_yaml_path, "w", encoding="utf-8") as yaml_file:
        yaml.dump(processed_recipe, yaml_file)

    dataset.generate_quickcharts(resource=resource_name, path=temp_yaml_path)

    # delete the temp file
    if os.path.exists(temp_yaml_path):
        os.remove(temp_yaml_path)

    status = (
        f"Added Quick Chart defined in '{hdx_hxl_preview_file_path}' "
        f"to dataset '{dataset['name']}', resource '{resource_name}'"
    )
    return dataset, status


def get_date_range_from_timeseries_file(
    dataset_attributes: dict, output_directory: str = None
) -> str:
    date_range = ""
    if output_directory is None:
        output_directory = os.path.join(os.path.dirname(__file__), "output")

    # Default behaviour for litpop and crop production since they have no timeseries
    # The target reference year for the original,published litpop data is 2018 however the
    # population file it uses is 2020. The alternate method is 2018 throughout.
    if "litpop" in dataset_attributes["name"]:
        date_range = "[2020-01-01T00:00:00 TO 2020-12-31T23:59:59]"
    # There is ref_year metadata in the crop_production exposure object
    elif "crop-production" in dataset_attributes["name"]:
        date_range = "[2018-01-01T00:00:00 TO 2018-12-31T23:59:59]"
    else:
        # find filename for timeseries
        timeseries_attributes = read_attributes(
            dataset_attributes["name"].replace("dataset", "timeseries")
        )
        timeseries_filename = timeseries_attributes["filename_template"]
        timeseries_path = os.path.join(
            output_directory, dataset_attributes["output_subdirectory"], timeseries_filename
        )
        # Load file if it exists
        timeseries_data = pandas.read_csv(timeseries_path, low_memory=False)
        timeseries_data.drop(timeseries_data.head(1).index, inplace=True)

        # Extract first and last date
        start_date = timeseries_data["event_date"].min()
        end_date = timeseries_data["event_date"].max()

        date_range = f"[{start_date} TO {end_date}]"

    return date_range


def get_date_range_from_hdx(indicator: str, hdx_site: str = "stage", dataset_name: str = None):
    configure_hdx_connection(hdx_site=hdx_site)
    if dataset_name is None:
        dataset_name = f"climada-{indicator}-dataset"

    dataset = Dataset.read_from_hdx(dataset_name)

    if dataset is None:
        date_range = "No date on HDX"
    else:
        date_range = dataset["dataset_date"]

    return date_range


if __name__ == "__main__":
    DATASET_NAME = "climada-earthquake-dataset"
    DRY_RUN = True
    create_datasets_in_hdx(DATASET_NAME, dry_run=DRY_RUN)
