#!/usr/bin/env python
# encoding: utf-8
import datetime
import logging
import sys

from typing import Any

import geopandas
import pandas as pd
import numpy as np

from hdx.utilities.easy_logging import setup_logging
from hdx.location.country import Country

from climada.util.api_client import Client
import climada.util.coordinates as u_coord
from climada.entity import LitPop

from hdx_scraper_climada.download_admin1_geometry import (
    get_admin1_shapes_from_hdx,
    get_admin2_shapes_from_hdx,
)


CLIENT = Client()

setup_logging()
LOGGER = logging.getLogger(__name__)

GLOBAL_INDICATOR_CACHE = {}


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
        if len(dataset.key_reference) != 0:
            print(f"Key reference: {dataset.key_reference[0]['key_reference']}", flush=True)
        else:
            print("Key reference:", flush=True)
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
    elif indicator == "crop-production":
        admin1_indicator_gdf = calculate_crop_production_for_admin1(admin1_shape, country)
    elif indicator == "earthquake":
        admin1_indicator_gdf = calculate_earthquake_for_admin1(admin1_shape, country)
    elif indicator == "flood":
        admin1_indicator_gdf = calculate_flood_for_admin1(admin1_shape, country)
    elif indicator == "relative-cropyield":
        admin1_indicator_gdf = calculate_relative_cropyield_for_admin1(admin1_shape, country)
    else:
        LOGGER.info(f"Indicator {indicator} is not yet implemented")
        raise NotImplementedError

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
    # Round all values - reconsider for each dataset.
    if indicator == "earthquake":
        admin1_indicator_gdf["value"] = admin1_indicator_gdf["value"].round(2)
    else:
        admin1_indicator_gdf["value"] = admin1_indicator_gdf["value"].round(0)

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


def calculate_litpop_alt_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
    indicator: str,
) -> pd.DataFrame:
    country_iso_numeric = get_country_iso_numeric(country)
    admin1_indicator_data = CLIENT.get_exposures(
        "litpop",
        properties={
            "country_iso3num": str(country_iso_numeric),
            "exponents": "(1,1)",
            "fin_mode": "pc",
        },
    )

    admin1_indicator_gdf = admin1_indicator_data.gdf.reset_index()

    admin1_indicator_gdf = filter_dataframe_with_geometry(
        admin1_indicator_gdf, admin1_shape, indicator
    )

    return admin1_indicator_gdf


def calculate_crop_production_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
) -> pd.DataFrame:
    # Global data is cached in a dictionary GLOBAL_INDICATOR_CACHE keyed by the indicator name
    # the `global` keyword is not required because we mutate this dictionary rather than reassign.

    crops = ["mai", "whe", "soy", "ric"]
    irrigation_statuses = ["noirr", "firr"]
    crop_gdfs = []
    for crop in crops:
        for irrigation_status in irrigation_statuses:
            indicator_key = f"crop-production.{crop}.{irrigation_status}.USD"
            if indicator_key not in GLOBAL_INDICATOR_CACHE:
                admin1_indicator_data = CLIENT.get_exposures(
                    "crop_production",
                    properties={
                        "crop": crop,
                        "irrigation_status": irrigation_status,
                        "unit": "USD",
                        "spatial_coverage": "global",
                    },
                )
                GLOBAL_INDICATOR_CACHE[indicator_key] = admin1_indicator_data
            else:
                admin1_indicator_data = GLOBAL_INDICATOR_CACHE[indicator_key]

            admin1_indicator_gdf = admin1_indicator_data.gdf.reset_index()
            country_iso_numeric = get_country_iso_numeric(country)
            admin1_indicator_gdf = admin1_indicator_gdf[
                admin1_indicator_gdf["region_id"] == country_iso_numeric
            ]
            admin1_indicator_gdf = filter_dataframe_with_geometry(
                admin1_indicator_gdf, admin1_shape, indicator_key
            )
            if len(admin1_indicator_gdf) == 0:
                # Calculate centroid of region
                centroid = calculate_centroid(admin1_shape)

                admin1_indicator_gdf = pd.DataFrame(
                    [
                        {
                            "latitude": round(centroid[0].y, 2),
                            "longitude": round(centroid[0].x, 2),
                            "indicator": indicator_key,
                            "value": 0.0,
                        }
                    ]
                )
            crop_gdfs.append(admin1_indicator_gdf)

    admin1_indicator_gdf = pd.concat(crop_gdfs, axis=0, ignore_index=True)

    return admin1_indicator_gdf


def calculate_earthquake_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
) -> pd.DataFrame:
    indicator_key = "earthquake.max_intensity"
    country_iso3alpha = Country.get_iso3_country_code(country)
    admin1_indicator_data = CLIENT.get_hazard(
        "earthquake",
        properties={
            "country_iso3alpha": country_iso3alpha,
        },
    )

    latitudes = admin1_indicator_data.centroids.lat.round(5)
    longitudes = admin1_indicator_data.centroids.lon.round(5)
    max_intensity = np.max(admin1_indicator_data.intensity, axis=0).toarray().flatten()
    admin1_indicator_gdf = pd.DataFrame(
        {"latitude": latitudes, "longitude": longitudes, "value": max_intensity}
    )
    # admin1_indicator_gdf = admin1_indicator_data.gdf.reset_index()

    admin1_indicator_gdf = filter_dataframe_with_geometry(
        admin1_indicator_gdf, admin1_shape, indicator_key
    )

    return admin1_indicator_gdf


def calculate_earthquake_timeseries_admin2(
    country: str,
):
    LOGGER.info(f"Creating timeseries summary for earthquakes in {country}")
    country_iso3alpha = Country.get_iso3_country_code(country)
    admin_level = "2"
    admin1_names, admin2_names, admin_shapes = get_admin2_shapes_from_hdx(country_iso3alpha)
    if len(admin2_names) == 0:
        LOGGER.info(f"No admin2 level shape data available for {country}")
        admin1_names, admin_shapes = get_admin1_shapes_from_hdx(country_iso3alpha)
        admin_level = "1"
        admin2_names = len(admin1_names) * [""]

    LOGGER.info(f"Found {len(admin2_names)} admin{admin_level} for {country}")
    indicator_key = "earthquake.max_intensity"

    earthquake = CLIENT.get_hazard(
        "earthquake",
        properties={
            "country_iso3alpha": country_iso3alpha,
        },
    )
    latitudes = earthquake.centroids.lat
    longitudes = earthquake.centroids.lon
    earthquakes = []
    for i, event_intensity in enumerate(earthquake.intensity):
        values = event_intensity.toarray().flatten()
        if sum(values) == 0.0:
            continue
        country_data = pd.DataFrame(
            {
                "latitude": latitudes,
                "longitude": longitudes,
                "value": values,
            }
        )

        for j, admin_shape in enumerate(admin_shapes):
            admin_indicator_gdf = filter_dataframe_with_geometry(
                country_data, admin_shape, indicator_key
            )

            if len(admin_indicator_gdf["value"]) != 0:
                max_intensity = max(admin_indicator_gdf["value"])
            else:
                max_intensity = 0.0
            if max_intensity > 0.0:
                event_date = datetime.datetime.fromordinal(earthquake.date[i]).isoformat()[0:10]
                LOGGER.info(f"Events found for {event_date} in {admin1_names[j]}-{admin2_names[j]}")
                print(
                    country, admin1_names[j], admin2_names[j], event_date, max_intensity, flush=True
                )
                earthquakes.append(
                    {
                        "country_name": country,
                        "admin1_name": admin1_names[j],
                        "admin2_name": admin2_names[j],
                        "latitude": round(admin_indicator_gdf["latitude"].mean(), 4),
                        "longitude": round(admin_indicator_gdf["longitude"].mean(), 4),
                        "aggregation": "max",
                        "indicator": "earthquake.date.max_intensity",
                        "event_date": event_date,
                        "value": round(max_intensity, 2),
                    }
                )

    return earthquakes


def calculate_flood_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
) -> pd.DataFrame:
    indicator_key = "flood.max_intensity"
    country_iso3alpha = Country.get_iso3_country_code(country)
    admin1_indicator_data = CLIENT.get_hazard(
        "flood",
        properties={
            "country_iso3alpha": country_iso3alpha,
        },
    )

    latitudes = admin1_indicator_data.centroids.lat.round(5)
    longitudes = admin1_indicator_data.centroids.lon.round(5)
    max_intensity = np.max(admin1_indicator_data.intensity, axis=0).toarray().flatten()
    admin1_indicator_gdf = pd.DataFrame(
        {"latitude": latitudes, "longitude": longitudes, "value": max_intensity}
    )
    # admin1_indicator_gdf = admin1_indicator_data.gdf.reset_index()

    admin1_indicator_gdf = filter_dataframe_with_geometry(
        admin1_indicator_gdf, admin1_shape, indicator_key
    )

    return admin1_indicator_gdf


def calculate_relative_cropyield_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
) -> pd.DataFrame:
    # Global data is cached in a dictionary GLOBAL_INDICATOR_CACHE keyed by the indicator name
    # the `global` keyword is not required because we mutate this dictionary rather than reassign.

    crops = ["mai", "whe", "soy", "ric"]
    irrigation_statuses = ["noirr", "firr"]
    crop_gdfs = []
    for crop in crops:
        for irrigation_status in irrigation_statuses:
            indicator_key = f"crop-production.{crop}.{irrigation_status}.USD"
            if crop == "soy":
                year_range = "1980_2012"
            else:
                year_range = "1971_2001"

            if indicator_key not in GLOBAL_INDICATOR_CACHE:
                admin1_indicator_data = CLIENT.get_hazard(
                    "relative_cropyield",
                    properties={
                        "climate_scenario": "historical",
                        "crop": crop,
                        "irrigation_status": irrigation_status,
                        "res_arcsec": "1800",
                        "spatial_coverage": "global",
                        "year_range": year_range,
                    },
                )
                GLOBAL_INDICATOR_CACHE[indicator_key] = admin1_indicator_data
            else:
                admin1_indicator_data = GLOBAL_INDICATOR_CACHE[indicator_key]

            return admin1_indicator_data

            admin1_indicator_gdf = admin1_indicator_data.gdf.reset_index()
            country_iso_numeric = get_country_iso_numeric(country)
            admin1_indicator_gdf = admin1_indicator_gdf[
                admin1_indicator_gdf["region_id"] == country_iso_numeric
            ]
            admin1_indicator_gdf = filter_dataframe_with_geometry(
                admin1_indicator_gdf, admin1_shape, indicator_key
            )
            if len(admin1_indicator_gdf) == 0:
                # Calculate centroid of region
                centroid = calculate_centroid(admin1_shape)

                admin1_indicator_gdf = pd.DataFrame(
                    [
                        {
                            "latitude": round(centroid[0].y, 2),
                            "longitude": round(centroid[0].x, 2),
                            "indicator": indicator_key,
                            "value": 0.0,
                        }
                    ]
                )
            crop_gdfs.append(admin1_indicator_gdf)

    admin1_indicator_gdf = pd.concat(crop_gdfs, axis=0, ignore_index=True)

    return admin1_indicator_gdf


def get_country_iso_numeric(country):
    if country.lower() == "dr congo":
        country_iso_numeric = 180
    elif country.lower() == "state of palestine":
        country_iso_numeric = 275
    else:
        country_iso_numeric = u_coord.country_to_iso(country, "numeric")
    return country_iso_numeric


def calculate_centroid(admin1_shape: list[geopandas.geoseries.GeoSeries]) -> Any:
    centroids = []
    for shp in admin1_shape:
        centroids.append(shp.centroid)
    gdf = geopandas.GeoDataFrame({}, geometry=centroids)
    centroid = gdf.dissolve().centroid
    return centroid


def filter_dataframe_with_geometry(
    admin1_indicator_gdf: pd.DataFrame,
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    indicator_key: str,
) -> pd.DataFrame:
    admin1_indicator_geo_gdf = geopandas.GeoDataFrame(
        admin1_indicator_gdf,
        geometry=geopandas.points_from_xy(
            admin1_indicator_gdf.longitude, admin1_indicator_gdf.latitude
        ),
    )

    indices_within = pd.Series(False, index=admin1_indicator_geo_gdf.index)
    for shp in admin1_shape:
        indices_within = indices_within | admin1_indicator_geo_gdf.geometry.within(shp)

    admin1_indicator_geo_gdf = admin1_indicator_geo_gdf.loc[indices_within]

    admin1_indicator_geo_gdf["indicator"] = len(admin1_indicator_geo_gdf) * [indicator_key]
    admin1_indicator_geo_gdf = admin1_indicator_geo_gdf[
        ["latitude", "longitude", "indicator", "value"]
    ]

    if len(admin1_indicator_geo_gdf) == 0:
        LOGGER.info("No rows inside geometry filter")
    return admin1_indicator_geo_gdf


if __name__ == "__main__":
    DATA_TYPE = "litpop"
    if len(sys.argv) == 2:
        DATA_TYPE = sys.argv[1]
    print_overview_information(data_type=DATA_TYPE)
