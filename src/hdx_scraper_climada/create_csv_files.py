#!/usr/bin/env python
# encoding: utf-8

import datetime
import os
import time

from collections import OrderedDict

import pandas as pd

from hdx.location.country import Country

from climada.util.api_client import Client

from climada.entity import LitPop

from hdx_scraper_climada.utilities import write_dictionary, read_countries
from hdx_scraper_climada.download_admin1_geometry import (
    get_admin1_shapes_from_hdx,
    get_admin1_shapes_from_natural_earth,
)

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
    country: str = "Haiti", indicator: str = "litpop", use_hdx_admin1: bool = True
):
    t0 = time.time()
    print(f"\nProcessing {country}", flush=True)
    # Construct file path
    country_str = country.lower().replace(" ", "-")
    output_file_path = os.path.join(
        os.path.dirname(__file__), "output", f"{indicator}", f"{country_str}-admin1-{indicator}.csv"
    )

    if os.path.exists(output_file_path):
        print(
            f"Output file {output_file_path} already exists, continuing to next country", flush=True
        )
        return None

    # Create the data
    country_dataframes = create_dataframes(country, indicator, use_hdx_admin1=use_hdx_admin1)

    # Make detail files
    country_litpop_gdf = pd.concat(country_dataframes, axis=0, ignore_index=True)
    hxl_tag_row = pd.DataFrame([HXL_TAGS])
    country_litpop_gdf = pd.concat([hxl_tag_row, country_litpop_gdf], axis=0, ignore_index=True)
    country_litpop_gdf.to_csv(
        output_file_path,
        index=False,
    )

    # Make summary file
    n_lines = len(country_litpop_gdf)
    summary_rows = create_summary_data(country_dataframes, country, indicator)
    status = write_summary_data(summary_rows, indicator)
    print(status, flush=True)

    print(
        f"Processing for {country} took {time.time()-t0:0.0f} seconds "
        f"and generated {n_lines} lines of output",
        flush=True,
    )

    return country_dataframes


def create_dataframes(country: str, indicator: str = "litpop", use_hdx_admin1: bool = True):
    country_iso3a = Country.get_iso3_country_code(country)
    # Get admin1 dataset
    if use_hdx_admin1:
        admin1_names, admin1_shapes = get_admin1_shapes_from_hdx(country_iso3a)
    else:
        admin1_names, admin1_shapes = get_admin1_shapes_from_natural_earth(country_iso3a)

    if len(admin1_names) == 0 and len(admin1_shapes) == 0:
        print(f"No Admin1 areas found for {country}", flush=True)
        return

    print("Admin1 areas in {country}:")
    print(admin1_names, flush=True)

    country_dataframes = []
    n_regions = len(admin1_shapes)
    for i, admin1_shape in enumerate(admin1_shapes, start=0):
        if admin1_names[i] is None:
            print("Admin1 name was 'None', continuing to next admin1", flush=True)
        print(f"{i+1} of {n_regions} Processing {admin1_names[i]}", flush=True)

        admin1_litpop = LitPop.from_shape_and_countries(admin1_shape, country_iso3a, res_arcsec=150)

        admin1_litpop_gdf = admin1_litpop.gdf
        admin1_litpop_gdf["region_name"] = len(admin1_litpop_gdf) * [admin1_names[i]]
        admin1_litpop_gdf["country_name"] = len(admin1_litpop_gdf) * [country]
        admin1_litpop_gdf["indicator"] = len(admin1_litpop_gdf) * [indicator]
        admin1_litpop_gdf["aggregation"] = len(admin1_litpop_gdf) * ["none"]

        # Restructure dataframe
        admin1_litpop_gdf.drop(["index", "region_id", "geometry", "impf_"], axis=1)
        admin1_litpop_gdf = admin1_litpop_gdf[
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

        print(f"Wrote {len(admin1_litpop_gdf)} lines", flush=True)
        country_dataframes.append(admin1_litpop_gdf)

    return country_dataframes


def create_summary_data(country_dataframes: list, country: str, indicator: str) -> list[dict]:
    summary_rows = []
    for df in country_dataframes:
        if len(df) == 0:
            print("Dataframe length is zero", flush=True)
            continue
        row = HXL_TAGS.copy()
        row["country_name"] = country
        row["region_name"] = df["region_name"][0]
        row["latitude"] = round(df["latitude"].mean(), 4)
        row["longitude"] = round(df["longitude"].mean(), 4)
        row["aggregation"] = "sum"
        row["indicator"] = indicator
        row["value"] = df["value"].sum()

        print(f"{df['region_name'][0]:<20}, {df['value'].sum():0.0f}", flush=True)
        summary_rows.append(row)

    return summary_rows


def write_summary_data(summary_rows: list, indicator: str) -> str:
    output_summary_path = os.path.join(
        os.path.dirname(__file__), "output", f"{indicator}", f"admin1-summaries-{indicator}.csv"
    )
    summary_rows = []
    if not os.path.exists(output_summary_path):
        summary_rows.append(HXL_TAGS)

    status = write_dictionary(
        output_summary_path,
        summary_rows,
        append=True,
    )
    return status


if __name__ == "__main__":
    print("Generating Climada csv files", flush=True)
    print("============================", flush=True)
    print(f"Timestamp: {datetime.datetime.now().isoformat()}", flush=True)
    T0 = time.time()
    ROWS = read_countries()
    for ROW in ROWS:
        export_indicator_data_to_csv(country=ROW["country_name"])

    print(f"Processed all countries in {time.time()-T0:0.0f} seconds")
    print(f"Timestamp: {datetime.datetime.now().isoformat()}", flush=True)
