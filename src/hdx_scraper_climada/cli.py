#!/usr/bin/env python
# encoding: utf-8

import logging
import os
import click

from hdx.utilities.easy_logging import setup_logging


from hdx_scraper_climada.climada_interface import print_overview_information
from hdx_scraper_climada.utilities import (
    read_attributes,
    INDICATOR_LIST,
    print_banner_to_log,
    CLIMADA_DISCLAIMER,
)
from hdx_scraper_climada.create_datasets import get_date_range_from_timeseries_file
from hdx_scraper_climada.download_from_hdx import (
    download_hdx_admin1_boundaries,
    download_hdx_datasets,
)
from hdx_scraper_climada.download_gpw_population_map import download_gpw_population
from hdx_scraper_climada.run import hdx_climada_run

setup_logging()
LOGGER = logging.getLogger(__name__)

INDICATOR_ALL = ["all"]
INDICATOR_ALL.extend(INDICATOR_LIST)


@click.group()
def hdx_climada() -> None:
    """Tools for the CLIMADA datasets in HDX"""


@hdx_climada.command(name="info")
@click.option(
    "--data_type",
    type=click.Choice(INDICATOR_LIST),
    is_flag=False,
    default="litpop",
    help=("an HDX-CLIMADA indicator"),
)
def info(data_type: str = "litpop"):
    """Show the data_type info from the CLIMADA interface"""
    print_banner_to_log(LOGGER, "info")
    data_type = data_type.replace("-", "_")
    print_overview_information(data_type=data_type)


@hdx_climada.command(name="dataset_date", short_help="Show dataset date ranges")
@click.option(
    "--indicator",
    type=click.Choice(INDICATOR_ALL),
    is_flag=False,
    default="all",
    help=("an HDX-CLIMADA indicator or 'all'"),
)
def dataset_date(indicator: str = "all"):
    """Show dataset date ranges read from the output timeseries files"""
    print_banner_to_log(LOGGER, "dataset_date")
    if indicator == "all":
        indicator_list = INDICATOR_LIST
    else:
        indicator_list = [indicator]
    for indicator_ in indicator_list:
        dataset_attributes = read_attributes(f"climada-{indicator_}-dataset")
        date_range = get_date_range_from_timeseries_file(dataset_attributes)
        indicator_str = f"{indicator_} dataset_date:"
        print(f"{indicator_str:<40} {date_range}", flush=True)


@hdx_climada.command(name="download")
@click.option(
    "--data_name",
    type=click.Choice(["boundaries", "population", "climada"]),
    is_flag=False,
    default="all",
    help="data to download",
)
@click.option(
    "--indicator",
    type=click.Choice(INDICATOR_ALL),
    is_flag=False,
    default="all",
    help=("an HDX-CLIMADA indicator or 'all'"),
)
@click.option(
    "--hdx_site",
    type=click.Choice(["stage", "prod"]),
    is_flag=False,
    default="stage",
    help="an hdx_site value",
)
@click.option("--download_directory", is_flag=False, default=None, help="target_directory")
def download(
    data_name: str = "boundaries",
    indicator: str = "litpop",
    hdx_site: str = "stage",
    download_directory: str = None,
):
    """Download data assets required to build the datasets"""
    print_banner_to_log(LOGGER, "download")
    if data_name == "boundaries":
        print(
            "Downloading admin boundaries from HDX requires an appropriate HDX API key", flush=True
        )
        resource_file_paths = download_hdx_admin1_boundaries(
            download_directory=download_directory, hdx_site=hdx_site
        )
        print(f"Downloaded admin1 boundary data to: {resource_file_paths}")
    elif data_name == "population":
        if ("NASA_EARTHDATA_USERNAME" not in os.environ) or (
            "NASA_EARTHDATA_USERNAME" not in os.environ
        ):
            print(
                "The population data download requires the environment variables "
                "NASA_EARTHDATA_USERNAME and NASA_EARTHDATA_PASSWORD to be defined. "
                "These credentials are created at https://urs.earthdata.nasa.gov/"
            )

        download_gpw_population(target_directory=download_directory)
    elif data_name == "climada":
        # We would handle "all" here by listing indicators and looping over them
        # Make dataset name
        print(CLIMADA_DISCLAIMER, flush=True)
        dataset_name = f"climada-{indicator}-dataset"
        if download_directory is None:
            download_directory = os.path.join(os.path.dirname(__file__), "output")
        download_subdirectory = os.path.join(download_directory, indicator)
        download_paths = download_hdx_datasets(
            dataset_filter=dataset_name,
            resource_filter=None,
            hdx_site=hdx_site,
            download_directory=download_subdirectory,
        )
        print("The following files were downloaded:", flush=True)
        for download_path in download_paths:
            print(download_path, flush=True)
    else:
        print(
            f"Data_name '{data_name}' is not know, only 'boundaries', 'population' and 'climada' "
            "are supported"
        )


@hdx_climada.command(name="create_dataset", short_help="Create a dataset in HDX with CSV files")
@click.option(
    "--indicator",
    type=click.Choice(INDICATOR_LIST),
    is_flag=False,
    default="litpop",
    help=("an HDX-CLIMADA indicator"),
)
@click.option(
    "--country",
    is_flag=False,
    default="all",
    help="A country name, currently unused the default value is 'all'",
)
@click.option(
    "--hdx_site",
    type=click.Choice(["stage", "prod"]),
    is_flag=False,
    default="stage",
    help="an hdx_site value",
)
@click.option(
    "--live",
    is_flag=True,
    default=False,
    help="if present then update to HDX is made, if absent then a dry run is done",
)
def create_dataset(
    indicator: str = "litpop", country: str = "all", hdx_site: str = "stage", live: bool = False
):
    """Create CSV data files for an indicator and create dataset in HDX"""
    print_banner_to_log(LOGGER, "create_dataset")
    hdx_climada_run(indicator, "all", hdx_site=hdx_site, dry_run=not live)
