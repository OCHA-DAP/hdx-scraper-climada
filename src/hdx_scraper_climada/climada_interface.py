#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import logging
import os
import sys

from typing import Any

import climada
import geopandas
import pandas as pd
import numpy as np

from hdx.utilities.easy_logging import setup_logging
from hdx.location.country import Country


from climada.util.api_client import Client
import climada.util.coordinates as u_coord
from climada.entity import LitPop

from hdx_scraper_climada.download_admin1_geometry import (
    get_best_admin_shapes,
    get_admin1_shapes_from_hdx,
    get_admin2_shapes_from_hdx,
)


CLIENT = Client()

setup_logging()
LOGGER = logging.getLogger(__name__)

GLOBAL_INDICATOR_CACHE = {}
SPATIAL_FILTER_CACHE = {}
CACHE_HIT = 0
CACHE_MISS = 0


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

    print("\nAvailable property values")
    for item in dataset_default.items():
        if len(item[1]) > 10:
            print(f"{item[0]}: {item[1][0:10]}\b... {len(item[1])} entries", flush=True)
        else:
            print(f"{item[0]}: {item[1]}", flush=True)

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
        admin1_indicator_gdf = calculate_hazards_for_admin1(admin1_shape, country, indicator)
    elif indicator == "flood":
        admin1_indicator_gdf = calculate_hazards_for_admin1(admin1_shape, country, indicator)
    elif indicator == "wildfire":
        admin1_indicator_gdf = calculate_hazards_for_admin1(admin1_shape, country, indicator)
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


def calculate_hazards_for_admin1(
    admin1_shape: list[geopandas.geoseries.GeoSeries],
    country: str,
    indicator: str,
) -> pd.DataFrame:
    """This function calculates detail data for the earthquake, flood, and wildfire datasets

    Arguments:
        admin1_shape {list[geopandas.geoseries.GeoSeries]} -- _description_
        country {str} -- full name of country - it is generally converted to iso3_country_code
        indicator {str} --

    Returns:
        pd.DataFrame -- _description_
    """

    indicator_key = indicator
    if indicator == "earthquake":
        indicator_key = "earthquake.max_intensity"
    elif indicator == "flood":
        indicator_key = "flood.max_intensity"

    country_iso3alpha = Country.get_iso3_country_code(country)
    admin1_indicator_data = CLIENT.get_hazard(
        indicator,
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

    # Filter out zero entries to reduce the file size for flood
    if indicator in ["flood"]:
        admin1_indicator_gdf = admin1_indicator_gdf[admin1_indicator_gdf["value"] != 0.0]

    admin1_indicator_gdf = filter_dataframe_with_geometry(
        admin1_indicator_gdf, admin1_shape, indicator_key
    )

    return admin1_indicator_gdf


def calculate_indicator_timeseries_admin(
    country: str, indicator: str = "earthquake", test_run: bool = False
) -> list[dict]:
    global SPATIAL_FILTER_CACHE
    SPATIAL_FILTER_CACHE = {}
    LOGGER.info(f"Creating timeseries summary for {indicator} in {country}")
    country_iso3alpha = Country.get_iso3_country_code(country)

    admin1_names, admin2_names, admin_shapes, admin_level = get_best_admin_shapes(country_iso3alpha)

    LOGGER.info(f"Found {len(admin2_names)} admin{admin_level} for {country}")
    if indicator == "earthquake":
        indicator_key = f"{indicator}.date.max_intensity"
    elif indicator == "flood":
        indicator_key = f"{indicator}.date"
    elif indicator == "wildfire":
        indicator_key = f"{indicator}.date"

    indicator_data = CLIENT.get_hazard(
        indicator,
        properties={
            "country_iso3alpha": country_iso3alpha,
        },
    )

    if indicator == "flood" and country in ["Colombia", "Nigeria", "Sudan", "Venezuela"]:
        indicator_data = flood_timeseries_data_shim(indicator_data)
    latitudes = indicator_data.centroids.lat
    longitudes = indicator_data.centroids.lon
    events = []
    n_events = indicator_data.intensity.shape[0]
    n_shapes = len(admin_shapes)
    for i, event_intensity in enumerate(indicator_data.intensity):
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

        # Flood is on a 200mx200m grid so we filter out zero values before processing, this
        # means we can't cache the spatial filters
        if indicator == "flood":
            country_data = country_data[country_data["value"] != 0]
        LOGGER.info(f"**Processing event {i} of {n_events}**")
        for j, admin_shape in enumerate(admin_shapes):
            # Switch off caching for flood because the filter above stops it working
            if indicator == "flood":
                cache_key = None
            else:
                cache_key = f"{admin1_names[j]}-{admin2_names[j]}"
            admin_indicator_gdf = filter_dataframe_with_geometry(
                country_data, admin_shape, indicator_key, cache_key=cache_key
            )
            LOGGER.info(
                f"Processing shape {j} of {n_shapes} "
                f"{country_iso3alpha}-{admin1_names[j]}-{admin2_names[j]}"
            )

            if indicator in ["earthquake"]:
                aggregation = "max"
                if len(admin_indicator_gdf["value"]) != 0:
                    aggregate = round(max(admin_indicator_gdf["value"]), 2)
                else:
                    aggregate = 0.0
            elif indicator in ["wildfire"]:
                aggregation = "sum"
                if len(admin_indicator_gdf["value"]) != 0:
                    mask_df = admin_indicator_gdf[admin_indicator_gdf["value"] != 0]
                    aggregate = round(len(mask_df), 2)
                else:
                    aggregate = 0.0
            else:
                aggregation = "sum"
                if len(admin_indicator_gdf["value"]) != 0:
                    aggregate = round(sum(admin_indicator_gdf["value"]), 0)
                else:
                    aggregate = 0.0
            if aggregate > 0.0:
                event_date = datetime.datetime.fromordinal(indicator_data.date[i]).isoformat()
                LOGGER.info(f"Event on {event_date[0:10]}  MaxInt:{aggregate:0.2f}")
                events.append(
                    {
                        "country_name": country,
                        "admin1_name": admin1_names[j],
                        "admin2_name": admin2_names[j],
                        "latitude": round(admin_indicator_gdf["latitude"].mean(), 4),
                        "longitude": round(admin_indicator_gdf["longitude"].mean(), 4),
                        "aggregation": aggregation,
                        "indicator": indicator_key,
                        "event_date": event_date,
                        "value": aggregate,
                    }
                )
        if test_run:
            break

    print(f"CACHE_HIT: {CACHE_HIT}", flush=True)
    print(f"CACHE_MISS: {CACHE_MISS}", flush=True)
    return events


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
    cache_key: str = None,
) -> pd.DataFrame:
    global CACHE_HIT, CACHE_MISS
    admin1_indicator_geo_gdf = geopandas.GeoDataFrame(
        admin1_indicator_gdf,
        geometry=geopandas.points_from_xy(
            admin1_indicator_gdf.longitude, admin1_indicator_gdf.latitude
        ),
    )

    if cache_key is not None and cache_key in SPATIAL_FILTER_CACHE:
        indices_within = SPATIAL_FILTER_CACHE[cache_key]
        CACHE_HIT += 1
    else:
        indices_within = pd.Series(False, index=admin1_indicator_geo_gdf.index)
        for shp in admin1_shape:
            indices_within = indices_within | admin1_indicator_geo_gdf.geometry.within(shp)
        SPATIAL_FILTER_CACHE[cache_key] = indices_within
        CACHE_MISS += 1

    admin1_indicator_geo_gdf = admin1_indicator_geo_gdf.loc[indices_within]

    admin1_indicator_geo_gdf["indicator"] = len(admin1_indicator_geo_gdf) * [indicator_key]
    admin1_indicator_geo_gdf = admin1_indicator_geo_gdf[
        ["latitude", "longitude", "indicator", "value"]
    ]

    if len(admin1_indicator_geo_gdf) == 0:
        LOGGER.info("No rows inside geometry filter")
    return admin1_indicator_geo_gdf


def flood_timeseries_data_shim(
    flood_data: climada.hazard.base.Hazard,
) -> climada.hazard.base.Hazard:
    # This shim takes flood event date information from a file and puts it into a Hazard object to
    # replace malformed event date. Described in this issue on the CLIMADA repo
    # https://github.com/CLIMADA-project/climada_python/issues/850
    # The countries effected are Colombia, Nigeria, Sudan and Venezuela for flood data
    # The lookup is from a dfo event number to an ordinal date
    shim_file_path = os.path.join(
        os.path.dirname(__file__), "metadata", "2024-02-21-flood_metainfo-ex-em.csv"
    )

    date_lookup = {}
    with open(shim_file_path, "r", encoding="utf-8") as shim_file:
        rows = csv.DictReader(shim_file)

        for row in rows:
            date_lookup[f"DFO_{row['id']}"] = row["date"]

    new_date_list = []

    for event_id in flood_data.event_name:
        new_date_list.append(int(date_lookup[event_id]))

    flood_data.date = new_date_list

    return flood_data


if __name__ == "__main__":
    DATA_TYPE = "litpop"
    if len(sys.argv) == 2:
        DATA_TYPE = sys.argv[1]
    print_overview_information(data_type=DATA_TYPE)
