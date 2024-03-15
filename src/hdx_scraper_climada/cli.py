#!/usr/bin/env python
# encoding: utf-8

import click

from hdx_scraper_climada.climada_interface import print_overview_information
from hdx_scraper_climada.utilities import read_attributes, INDICATOR_LIST
from hdx_scraper_climada.create_datasets import get_date_range_from_timeseries_file
from hdx_scraper_climada.download_admin_geometries_from_hdx import download_hdx_admin1_boundaries
from hdx_scraper_climada.download_gpw_population_map import download_gpw_population
from hdx_scraper_climada.run import hdx_climada_run


@click.group()
def hdx_climada() -> None:
    """Tools for the CLIMADA datasets in HDX"""


@hdx_climada.command(name="info")
@click.option(
    "--data_type",
    is_flag=False,
    default="litpop",
    help=(
        "a CLIMADA data_type in the set "
        "{litpop|crop_production|earthquake|flood|wildfire|tropical_cyclone|storm_europe}"
    ),
)
def info(data_type: str = "litpop"):
    """Show the data_type info from the CLIMADA interface"""
    print_overview_information(data_type=data_type)


@hdx_climada.command(name="dataset_date")
@click.option(
    "--indicator",
    is_flag=False,
    default="all",
    help=(
        "an HDX-CLIMADA in the set "
        "{all|litpop|crop_production|earthquake|flood|wildfire|tropical_cyclone|storm_europe}"
    ),
)
def dataset_date(indicator: str = "all"):
    """Show dataset date ranges read from the output timeseries files"""
    if indicator == "all":
        indicator_list = INDICATOR_LIST
    else:
        indicator_list = [indicator]
    for indicator in indicator_list:
        dataset_attributes = read_attributes(f"climada-{indicator}-dataset")
        date_range = get_date_range_from_timeseries_file(dataset_attributes)
        print(f"{indicator}: {date_range}", flush=True)


@hdx_climada.command(name="download")
@click.option(
    "--data_name",
    is_flag=False,
    default="all",
    help="data to download, one of {boundaries|population}",
)
@click.option("--download_directory", is_flag=False, default=None, help="target_directory")
def download(data_name: str = "boundaries", download_directory: str = None):
    """Download data assets required to build the datasets"""

    if data_name == "boundaries":
        print("Downloading admin boundaries from HDX requires an appropriate HDX API key")
        resource_file_paths = download_hdx_admin1_boundaries(download_directory=download_directory)
        print(f"Downloaded admin1 boundary data to: {resource_file_paths}")
    elif data_name == "population":
        print(
            "The population data download requires the environment variables "
            "NASA_EARTHDATA_USERNAME and NASA_EARTHDATA_PASSWORD to be defined. "
            "These credentials are created at https://urs.earthdata.nasa.gov/"
        )
        download_gpw_population(target_directory=download_directory)
    else:
        print(
            f"Data_name '{data_name}' is not know, only 'boundaries' and 'population' are supported"
        )


@hdx_climada.command(name="create_dataset")
@click.option(
    "--indicator",
    is_flag=False,
    default="all",
    help=(
        "an HDX-CLIMADA in the set "
        "{litpop|crop_production|earthquake|flood|wildfire|tropical_cyclone|storm_europe}"
    ),
)
@click.option(
    "--country",
    is_flag=False,
    default="all",
    help="A country name, currently unused the default value is 'all'",
)
@click.option(
    "--hdx_site",
    is_flag=False,
    default="stage",
    help="an hdx_site value {stage|prod}",
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
    hdx_climada_run(indicator, "all", hdx_site=hdx_site, dry_run=not live)
