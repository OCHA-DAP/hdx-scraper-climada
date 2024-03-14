#!/usr/bin/env python
# encoding: utf-8

import click

from hdx_scraper_climada.climada_interface import print_overview_information


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
