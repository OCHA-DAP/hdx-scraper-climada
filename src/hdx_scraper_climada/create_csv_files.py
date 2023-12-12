#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import os
import time

from collections import OrderedDict

import pandas as pd

from hdx.data.dataset import Dataset
from hdx.location.country import Country

from climada.util.api_client import Client

import climada.util.coordinates as u_coord
from climada.entity import LitPop

from hdx_scraper_climada.utilities import write_dictionary
from hdx_scraper_climada.download_admin1_geometry import get_admin1_shapes_from_hdx

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


def print_overview_information(data_type="litpop"):
    data_types = CLIENT.list_data_type_infos()
    print("Available Data Types\n====================", flush=True)
    for dataset in data_types:
        print(f"{dataset.data_type} ({dataset.data_type_group})", flush=True)

    print(f"\nDetails for {data_type}\n=======================", flush=True)

    for dataset in data_types:
        if dataset.data_type != data_type:
            continue
        print(f"Data_type: {data_types[0].data_type}", flush=True)
        print(f"Data_type_group: {data_types[0].data_type_group}", flush=True)
        print(f"Description:\n {data_types[0].description} \n", flush=True)
        print(f"Key reference: {data_types[0].key_reference[0]['key_reference']}", flush=True)
        print("\nProperties:", flush=True)
        for i, property_ in enumerate(data_types[0].properties, start=1):
            print(f"{i}. {property_['property']}: {property_['description']}", flush=True)
        print(f"status: {data_types[0].status}", flush=True)
        print(f"version_notes: {data_types[0].version_notes[0]['version']}", flush=True)

    litpop_dataset_infos = CLIENT.list_dataset_infos(data_type="litpop")
    litpop_default = CLIENT.get_property_values(
        litpop_dataset_infos,
        known_property_values={"fin_mode": "pc", "exponents": "(1,1)"},
    )

    print(f"Available for {len(litpop_default['country_iso3alpha'])} countries", flush=True)


def export_litpop_data_to_csv(country: str = "Haiti", indicator: str = "litpop"):
    country_iso3a = Country.get_iso3_country_code(country)
    t0 = time.time()
    print(f"\nProcessing {country}", flush=True)
    output_file_path = os.path.join(
        os.path.dirname(__file__), "output", "litpop", f"{country}-admin1-litpop.csv"
    )

    if os.path.exists(output_file_path):
        print(
            f"Output file {output_file_path} already exists, continuing to next country", flush=True
        )
        return

    # Get whole country dataset
    # whole_country = LitPop.from_countries(country_iso3a)
    # whole_country.gdf.to_csv(
    #     os.path.join(
    #         os.path.dirname(__file__), "output", "litpop", f"{country}-country-litpop.csv"
    #     ),
    #     index=False,
    # )

    # Get admin1 dataset
    # admin1_names, admin1_shapes = get_admin1_shapes_from_natural_earth(country_iso3a)
    admin1_names, admin1_shapes = get_admin1_shapes_from_hdx(country_iso3a)

    if admin1_names is None and admin1_shapes is None:
        return

    print("Admin1 areas in this country:")
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

        # print(admin1_litpop_gdf[0:10], flush=True)
        print(f"Wrote {len(admin1_litpop_gdf)} lines", flush=True)
        country_dataframes.append(admin1_litpop_gdf)

    country_litpop_gdf = pd.concat(country_dataframes, axis=0, ignore_index=True)

    # print(country_litpop_gdf.columns.to_list(), flush=True)

    # Drop "df index", Index, geometry, impf_ columns
    country_litpop_gdf.drop(["index", "region_id", "geometry", "impf_"], axis=1)

    # Reorder to:
    # region_name, region_id, latitude, longitude, value
    country_litpop_gdf = country_litpop_gdf[
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

    # Skip HXL line for now since it messes up visualisation in PowerBI.
    # hxl_tag_row = pd.DataFrame([HXL_TAGS])
    # country_litpop_gdf = pd.concat([hxl_tag_row, country_litpop_gdf], axis=0, ignore_index=True)

    n_lines = len(country_litpop_gdf)
    country_litpop_gdf.to_csv(
        output_file_path,
        index=False,
    )

    summary_rows = []
    for df in country_dataframes:
        if len(df) == 0:
            print("Dataframe length is zero", flush=True)
            continue
        row = HXL_TAGS.copy()
        row["country_name"] = country
        row["region_name"] = df["region_name"][0]
        row["latitude"] = round(df["latitude"].mean(), 4)
        row["longitude"] = round(df["latitude"].mean(), 4)
        row["aggregation"] = "sum"
        row["indicator"] = indicator
        row["value"] = df["value"].sum()

        print(f"{df['region_name'][0]:<20}, {df['value'].sum():0.0f}", flush=True)
        summary_rows.append(row)

    status = write_dictionary(
        os.path.join(os.path.dirname(__file__), "output", "litpop", "admin1-summaries-litpop.csv"),
        summary_rows,
        append=True,
    )
    print(status, flush=True)

    # litpop = CLIENT.get_litpop(country=country)
    # # print(dir(litpop), flush=True)
    # print(litpop.gdf, flush=True)
    # litpop.gdf.to_csv(os.path.join(os.path.dirname(__file__), "output", "litpop.csv"))
    print(
        f"Processing for {country} took {time.time()-t0:0.0f} seconds and generated {n_lines} lines of output",
        flush=True,
    )


def get_admin1_shapes_from_natural_earth(country_iso3a):
    try:
        admin1_info, admin1_shapes = u_coord.get_admin1_info(country_iso3a)
    except LookupError as error:
        print(error, flush=True)
        return None, None

    admin1_info = admin1_info[country_iso3a]
    admin1_shapes = admin1_shapes[country_iso3a]

    admin1_names = [record["name"] for record in admin1_info]

    return admin1_names, admin1_shapes


if __name__ == "__main__":
    # print_overview_information(data_type="litpop")
    # export_litpop_data_to_csv(country="Haiti")
    print("Generating Climada csv files", flush=True)
    print("============================", flush=True)
    print(f"Timestamp: {datetime.datetime.now().isoformat()}", flush=True)
    T0 = time.time()
    with open(
        os.path.join(os.path.dirname(__file__), "metadata", "countries.csv"), encoding="utf-8"
    ) as COUNTRIES_FILE:
        ROWS = csv.DictReader(COUNTRIES_FILE)
        for ROW in ROWS:
            export_litpop_data_to_csv(country=ROW["country_name"])

    print(f"Processed all countries in {time.time()-T0:0.0f} seconds")
    print(f"Timestamp: {datetime.datetime.now().isoformat()}", flush=True)
