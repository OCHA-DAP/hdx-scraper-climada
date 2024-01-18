#!/usr/bin/env python
# encoding: utf-8

import datetime
import logging
import os
import time

from collections import OrderedDict, deque

import pandas as pd

from hdx.location.country import Country
from hdx.utilities.easy_logging import setup_logging

from climada.util.api_client import Client

from climada.entity import LitPop

from hdx_scraper_climada.utilities import write_dictionary, read_countries
from hdx_scraper_climada.download_admin1_geometry import (
    get_admin1_shapes_from_hdx,
    get_admin1_shapes_from_natural_earth,
)

setup_logging()
LOGGER = logging.getLogger(__name__)

CLIENT = Client()

HXL_TAGS = OrderedDict(
    [
        ("country_name", "#country"),
        ("region_name", "#adm1+name"),
        ("latitude", "#geo+lat"),
        ("longitude", "#geo+lon"),
        ("aggregation", ""),
        ("indicator", "#indicator+name"),
        ("value", "#indicator+num"),
    ]
)


def export_indicator_data_to_csv(
    country: str = "Haiti",
    indicator: str = "litpop",
    use_hdx_admin1: bool = True,
    export_directory: str = None,
) -> list[str]:
    statuses = []
    t0 = time.time()
    LOGGER.info(f"\nProcessing {country}")
    # Construct file paths
    output_detail_path, output_summary_path = make_detail_and_summary_file_paths(
        country, indicator, export_directory
    )

    if os.path.exists(output_detail_path):
        statuses.append(
            f"Output file {output_detail_path} already exists, continuing to next country"
        )
        return statuses

    # Make detail files
    country_dataframes = create_detail_dataframes(country, indicator, use_hdx_admin1=use_hdx_admin1)
    status = write_detail_data(country_dataframes, output_detail_path)
    statuses.append(status)

    # Make summary file
    summary_rows, n_lines = create_summary_data(country_dataframes, country, indicator)
    status = write_summary_data(summary_rows, output_summary_path)
    statuses.append(status)

    statuses.append(
        f"Processing for {country} took {time.time()-t0:0.0f} seconds "
        f"and generated {n_lines} lines of output",
    )

    return statuses


def make_detail_and_summary_file_paths(
    country: str, indicator: str, export_directory: str = None
) -> (str, str):
    if export_directory is None:
        export_directory = os.path.join(os.path.dirname(__file__), "output")
    country_str = country.lower().replace(" ", "-")
    output_detail_path = os.path.join(
        export_directory, f"{indicator}", f"{country_str}-admin1-{indicator}.csv"
    )

    output_summary_path = os.path.join(
        export_directory, f"{indicator}", f"admin1-summaries-{indicator}.csv"
    )

    indicator_directory = os.path.dirname(output_summary_path)
    if not os.path.exists(indicator_directory):
        LOGGER.info(f"Creating {os.path.dirname(output_summary_path)}")
        os.makedirs(os.path.dirname(output_summary_path), exist_ok=True)

    return output_detail_path, output_summary_path


def create_detail_dataframes(
    country: str, indicator: str = "litpop", use_hdx_admin1: bool = True
) -> list:
    country_iso3a = Country.get_iso3_country_code(country)
    # Get admin1 dataset
    if use_hdx_admin1:
        admin1_names, admin1_shapes = get_admin1_shapes_from_hdx(country_iso3a)
    else:
        admin1_names, admin1_shapes = get_admin1_shapes_from_natural_earth(country_iso3a)

    if len(admin1_names) == 0 and len(admin1_shapes) == 0:
        LOGGER.info(f"No Admin1 areas found for {country}")
        return []

    LOGGER.info(f"Admin1 areas in {country}:")
    LOGGER.info(admin1_names)

    country_dataframes = []
    n_regions = len(admin1_shapes)
    for i, admin1_shape in enumerate(admin1_shapes, start=0):
        if admin1_names[i] is None:
            LOGGER.info("Admin1 name was 'None', continuing to next admin1")
        LOGGER.info(f"{i+1} of {n_regions} Processing {admin1_names[i]}")

        admin1_indicator_gdf = calculate_indicator_for_admin1(
            admin1_shape, country_iso3a, indicator
        )

        admin1_indicator_gdf["region_name"] = len(admin1_indicator_gdf) * [admin1_names[i]]
        admin1_indicator_gdf["country_name"] = len(admin1_indicator_gdf) * [country]
        admin1_indicator_gdf["indicator"] = len(admin1_indicator_gdf) * [indicator]
        admin1_indicator_gdf["aggregation"] = len(admin1_indicator_gdf) * ["none"]

        # Restructure dataframe
        admin1_indicator_gdf.drop(["index", "region_id", "geometry", "impf_"], axis=1)
        admin1_indicator_gdf = admin1_indicator_gdf[
            [
                "country_name",
                "region_name",
                "latitude",
                "longitude",
                "aggregation",
                "indicator",
                "value",
            ]
        ]

        LOGGER.info(f"Wrote {len(admin1_indicator_gdf)} lines")
        country_dataframes.append(admin1_indicator_gdf)

    return country_dataframes


def calculate_indicator_for_admin1(admin1_shape, country_iso3a: str, indicator: str):
    admin1_indicator_gdf = None
    if indicator == "litpop":
        admin1_indicator_data = LitPop.from_shape_and_countries(
            admin1_shape, country_iso3a, res_arcsec=150
        )
        admin1_indicator_gdf = admin1_indicator_data.gdf
    else:
        LOGGER.info(f"Indicator {indicator} is not yet implemented")

    return admin1_indicator_gdf


def create_summary_data(
    country_dataframes: list[pd.DataFrame], country: str, indicator: str
) -> (list[dict], int):
    summary_rows = []
    n_lines = 0
    for df in country_dataframes:
        if len(df) == 0:
            LOGGER.info("Dataframe length is zero")
            continue
        n_lines += len(df)
        row = HXL_TAGS.copy()
        row["country_name"] = country
        row["region_name"] = df["region_name"][0]
        row["latitude"] = round(df["latitude"].mean(), 4)
        row["longitude"] = round(df["longitude"].mean(), 4)
        row["aggregation"] = "sum"
        row["indicator"] = indicator
        row["value"] = df["value"].sum()

        LOGGER.info(f"{df['region_name'][0]:<20}, {df['value'].sum():0.0f}")
        summary_rows.append(row)

    return summary_rows, n_lines


def write_summary_data(summary_rows: list, output_summary_path: str) -> str:
    if not os.path.exists(output_summary_path):
        # This is slightly convoluted, but efficient
        # https://www.geeksforgeeks.org/python-perform-append-at-beginning-of-list/
        summary_rows = deque(summary_rows)
        summary_rows.appendleft(HXL_TAGS)
        summary_rows = list(summary_rows)

    status = write_dictionary(
        output_summary_path,
        summary_rows,
        append=True,
    )
    return status


def write_detail_data(country_dataframes: list[pd.DataFrame], output_file_path: str) -> str:
    if os.path.exists(output_file_path):
        status = f"Output file {output_file_path} already exists, not overwriting"
        return status
    country_litpop_gdf = pd.concat(country_dataframes, axis=0, ignore_index=True)
    hxl_tag_row = pd.DataFrame([HXL_TAGS])
    country_litpop_gdf = pd.concat([hxl_tag_row, country_litpop_gdf], axis=0, ignore_index=True)
    country_litpop_gdf.to_csv(
        output_file_path,
        index=False,
    )

    status = f"Indicator data file written to {output_file_path}"

    return status


if __name__ == "__main__":
    LOGGER.info("Generating Climada csv files")
    LOGGER.info("============================")
    LOGGER.info(f"Timestamp: {datetime.datetime.now().isoformat()}")
    T0 = time.time()
    ROWS = read_countries()
    for ROW in ROWS:
        STATUSES = export_indicator_data_to_csv(country=ROW["country_name"])
        for STATUS in STATUSES:
            LOGGER.info(STATUS)
    LOGGER.info(f"Processed all countries in {time.time()-T0:0.0f} seconds")
    LOGGER.info(f"Timestamp: {datetime.datetime.now().isoformat()}")
