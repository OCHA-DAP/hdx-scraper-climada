#!/usr/bin/env python
# encoding: utf-8

import os
import json
from climada.util.api_client import Client
import pandas as pd
import climada.util.coordinates as u_coord
from climada.entity import LitPop


CLIENT = Client()


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


def export_litpop_data_to_csv(country: str = "Haiti"):
    country_iso3a = "HTI"
    admin1_info, admin1_shapes = u_coord.get_admin1_info(country_iso3a)

    admin1_info = admin1_info[country_iso3a]
    admin1_shapes = admin1_shapes[country_iso3a]

    admin1_names = [record["name"] for record in admin1_info]

    print(admin1_names, flush=True)

    haiti_dataframes = []
    for i, admin1_shape in enumerate(admin1_shapes, start=0):
        print(f"Processing {admin1_names[i]}", flush=True)
        admin1_litpop = LitPop.from_shape_and_countries(admin1_shape, country, res_arcsec=150)
        admin1_litpop_gdf = admin1_litpop.gdf
        admin1_litpop_gdf["region_name"] = len(admin1_litpop_gdf) * [admin1_names[i]]

        # haiti_litpop = haiti_litpop.append(admin1_litpop_gdf)
        print(admin1_litpop_gdf[0:10], flush=True)
        print(len(admin1_litpop_gdf), flush=True)
        haiti_dataframes.append(admin1_litpop_gdf)

    haiti_litpop_gdf = pd.concat(haiti_dataframes, axis=0, ignore_index=True)

    for df in haiti_dataframes:
        print(f"{df['region_name'][0]:<20}, {df.value.mean():0.0f}", flush=True)

    haiti_litpop_gdf.to_csv(
        os.path.join(os.path.dirname(__file__), "output", f"{country}-admin1-litpop.csv")
    )

    # litpop = CLIENT.get_litpop(country=country)
    # # print(dir(litpop), flush=True)
    # print(litpop.gdf, flush=True)
    # litpop.gdf.to_csv(os.path.join(os.path.dirname(__file__), "output", "litpop.csv"))


if __name__ == "__main__":
    # print_overview_information(data_type="litpop")
    export_litpop_data_to_csv(country="Haiti")
