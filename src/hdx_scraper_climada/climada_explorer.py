#!/usr/bin/env python
# encoding: utf-8

from climada.util.api_client import Client

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


if __name__ == "__main__":
    DATA_TYPE = "litpop"
    print_overview_information(data_type=DATA_TYPE)
