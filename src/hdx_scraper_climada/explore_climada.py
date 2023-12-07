#!/usr/bin/env python
# encoding: utf-8

import os
import json
from climada.util.api_client import Client
import pandas as pd


CLIENT = Client()


def main():
    data_types = CLIENT.list_data_type_infos()

    # print(type(data_types), flush=True)
    # print(data_types, flush=True)
    # print(dir(data_types[0]), flush=True)
    # for data_type in data_types:
    # print(json.dumps(data_types[0].properties, indent=4), flush=True)
    # dtf = pd.DataFrame(data_types)
    # dtf.sort_values(["data_type_group", "data_type"])

    # print(dtf, flush=True)
    litpop_dataset_infos = CLIENT.list_dataset_infos(data_type="litpop")
    litpop_default = CLIENT.get_property_values(
        litpop_dataset_infos,
        known_property_values={"fin_mode": "pc", "exponents": "(1,1)"},
    )

    # print(type(litpop_default), flush=True)
    # print(dir(litpop_default))
    # print(len(litpop_default["country_iso3alpha"]), flush=True)


def export_litpop_data_to_csv():
    litpop = CLIENT.get_litpop(country="Haiti")
    # print(dir(litpop), flush=True)
    print(litpop.gdf, flush=True)
    litpop.gdf.to_csv(os.path.join(os.path.dirname(__file__), "output", "litpop.csv"))


if __name__ == "__main__":
    export_litpop_data_to_csv()
