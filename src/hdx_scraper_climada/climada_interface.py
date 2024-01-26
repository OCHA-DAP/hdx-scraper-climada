#!/usr/bin/env python
# encoding: utf-8
import logging

import geopandas
import pandas as pd

from hdx.utilities.easy_logging import setup_logging

from climada.util.api_client import Client
import climada.util.coordinates as u_coord
from climada.entity import LitPop

CLIENT = Client()

setup_logging()
LOGGER = logging.getLogger(__name__)

CROP_PRODUCTION_CACHE = {}


def print_overview_information(data_type="litpop"):
    data_types = CLIENT.list_data_type_infos()
    print("Available Data Types\n====================", flush=True)
    for dataset in data_types:
        print(f"{dataset.data_type} ({dataset.data_type_group})", flush=True)

    print(f"\nDetails for {data_type}\n=======================", flush=True)

    for dataset in data_types:
        if dataset.data_type != data_type:
            continue
        print(f"Data_type: {dataset.data_type}", flush=True)
        print(f"Data_type_group: {dataset.data_type_group}", flush=True)
        print(f"Description:\n {dataset.description} \n", flush=True)
        print(f"Key reference: {dataset.key_reference[0]['key_reference']}", flush=True)
        print("\nProperties:", flush=True)
        for i, property_ in enumerate(dataset.properties, start=1):
            print(f"{i}. {property_['property']}: {property_['description']}", flush=True)
        print(f"status: {dataset.status}", flush=True)
        print(f"version_notes: {dataset.version_notes}", flush=True)

    dataset_infos = CLIENT.list_dataset_infos(data_type=data_type)
    dataset_default = CLIENT.get_property_values(dataset_infos)

    for item in dataset_default.items():
        print(item[0], item[1], flush=True)
    print(dataset_default, flush=True)

    # print(f"Available for {len(dataset_default['country_iso3alpha'])} countries", flush=True)


def calculate_indicator_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    admin1_name: str,
    country: str,
    indicator: str,
) -> pd.DataFrame:
    admin1_indicator_gdf = None
    if indicator == "litpop":
        admin1_indicator_gdf = calculate_litpop_for_admin1(admin1_shape, country, indicator)
    elif indicator == "litpop_alt":
        admin1_indicator_gdf = calculate_litpop_alt_for_admin1(admin1_shape, country, indicator)
    elif indicator == "crop_production":
        admin1_indicator_gdf = calculate_crop_production_for_admin1(
            admin1_shape, country, indicator
        )
    elif indicator == "relative_cropyield":
        admin1_indicator_gdf = calculate_relative_cropyield_for_admin1(
            admin1_shape, country, indicator
        )
    else:
        LOGGER.info(f"Indicator {indicator} is not yet implemented")

    admin1_indicator_gdf["region_name"] = len(admin1_indicator_gdf) * [admin1_name]
    admin1_indicator_gdf["country_name"] = len(admin1_indicator_gdf) * [country]
    admin1_indicator_gdf["aggregation"] = len(admin1_indicator_gdf) * ["none"]
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

    return admin1_indicator_gdf


def calculate_relative_cropyield_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
    indicator: str,
) -> pd.DataFrame:
    crop = "mai"
    irrigation_status = "noirr"
    indicator_key = f"{indicator}.{crop}.{irrigation_status}.USD"
    admin1_indicator_data = CLIENT.get_hazard(
        indicator,
        properties={
            "climate_scenario": "historical",
            "crop": crop,
            "irrigation_status": irrigation_status,
            "res_arcsec": "1800",
            "spatial_coverage": "global",
            "year_range": "1980_2012",
        },
    )
    admin1_indicator_gdf = admin1_indicator_data.gdf
    country_iso_numeric = u_coord.country_to_iso(country, "numeric")

    admin1_indicator_gdf = admin1_indicator_gdf[
        admin1_indicator_gdf["region_id"] == country_iso_numeric
    ]

    admin1_indicator_gdf = filter_dataframe_with_geometry(
        admin1_indicator_gdf, admin1_shape, indicator_key
    )

    return admin1_indicator_gdf


def calculate_crop_production_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
    indicator: str,
) -> pd.DataFrame:
    global CROP_PRODUCTION_CACHE
    crop = "mai"
    irrigation_status = "noirr"
    indicator_key = f"crop_production.{crop}.{irrigation_status}.USD"
    if indicator_key not in CROP_PRODUCTION_CACHE:
        admin1_indicator_data = CLIENT.get_exposures(
            indicator,
            properties={
                "crop": crop,
                "irrigation_status": irrigation_status,
                "unit": "USD",
                "spatial_coverage": "global",
            },
        )
        CROP_PRODUCTION_CACHE[indicator_key] = admin1_indicator_data
    else:
        admin1_indicator_data = CROP_PRODUCTION_CACHE[indicator_key]

    admin1_indicator_gdf = admin1_indicator_data.gdf.reset_index()
    country_iso_numeric = u_coord.country_to_iso(country, "numeric")
    admin1_indicator_gdf = admin1_indicator_gdf[
        admin1_indicator_gdf["region_id"] == country_iso_numeric
    ]
    # Geometry filter
    admin1_indicator_gdf = filter_dataframe_with_geometry(
        admin1_indicator_gdf, admin1_shape, indicator_key
    )

    return admin1_indicator_gdf


def calculate_litpop_alt_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
    indicator: str,
) -> pd.DataFrame:
    country_iso_numeric = u_coord.country_to_iso(country, "numeric")
    admin1_indicator_data = CLIENT.get_exposures(
        "litpop",
        properties={
            "country_iso3num": str(country_iso_numeric),
            "exponents": "(1,1)",
            "fin_mode": "pc",
        },
    )

    admin1_indicator_gdf = admin1_indicator_data.gdf.reset_index()

    # Geometry filter
    admin1_indicator_gdf = filter_dataframe_with_geometry(
        admin1_indicator_gdf, admin1_shape, indicator
    )

    return admin1_indicator_gdf


def calculate_litpop_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
    indicator: str,
) -> pd.DataFrame:
    admin1_indicator_data = LitPop.from_shape_and_countries(admin1_shape, country, res_arcsec=150)
    admin1_indicator_gdf = admin1_indicator_data.gdf
    admin1_indicator_gdf["indicator"] = len(admin1_indicator_gdf) * [indicator]
    admin1_indicator_gdf = admin1_indicator_gdf[["latitude", "longitude", "indicator", "value"]]
    return admin1_indicator_gdf


def filter_dataframe_with_geometry(
    admin1_indicator_gdf: pd.DataFrame,
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    indicator_key: str,
) -> pd.DataFrame:
    admin1_indicator_gdf = geopandas.GeoDataFrame(
        admin1_indicator_gdf,
        geometry=geopandas.points_from_xy(
            admin1_indicator_gdf.longitude, admin1_indicator_gdf.latitude
        ),
    )

    temp_gdf = []
    for shp in admin1_shape:
        temp_gdf.append(admin1_indicator_gdf.loc[admin1_indicator_gdf.geometry.within(shp)])
    admin1_indicator_gdf = pd.concat(temp_gdf)

    admin1_indicator_gdf["indicator"] = len(admin1_indicator_gdf) * [indicator_key]
    admin1_indicator_gdf = admin1_indicator_gdf[["latitude", "longitude", "indicator", "value"]]

    return admin1_indicator_gdf


if __name__ == "__main__":
    DATA_TYPE = "litpop"
    print_overview_information(data_type=DATA_TYPE)
