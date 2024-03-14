#!/usr/bin/env python
# encoding: utf-8

import click

from hdx_scraper_climada.climada_interface import print_overview_information
from hdx_scraper_climada.utilities import read_attributes, INDICATOR_LIST
from hdx_scraper_climada.create_datasets import get_date_range_from_timeseries_file


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
