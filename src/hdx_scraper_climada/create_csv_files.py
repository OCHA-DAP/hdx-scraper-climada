#!/usr/bin/env python
# encoding: utf-8

import datetime
import logging
import os
import time

from collections import OrderedDict, deque

import pandas as pd

from climada.util.api_client import Client

from hdx.location.country import Country
from hdx.utilities.easy_logging import setup_logging


from hdx_scraper_climada.utilities import (
    write_dictionary,
    HAS_TIMESERIES,
    get_set_of_countries_in_summary_file,
)
from hdx_scraper_climada.download_admin1_geometry import (
    get_admin1_shapes_from_hdx,
    get_admin1_shapes_from_natural_earth,
)

from hdx_scraper_climada.climada_interface import (
    calculate_indicator_for_admin1,
    calculate_indicator_timeseries_admin,
    aggregate_value,
)

setup_logging()
LOGGER = logging.getLogger(__name__)


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

TIMESERIES_HXL_TAGS = OrderedDict(
    [
        ("country_name", "#country"),
        ("admin1_name", "#adm1+name"),
        ("admin2_name", "#adm2+name"),
        ("latitude", "#geo+lat"),
        ("longitude", "#geo+lon"),
        ("aggregation", ""),
        ("indicator", "#indicator+name"),
        ("event_date", "#date"),
        ("value", "#indicator+num"),
    ]
)


def export_indicator_data_to_csv(
    country: str = "Haiti",
    indicator: str = "litpop",
    climate_scenario: str = None,
    use_hdx_admin1: bool = True,
    export_directory: str = None,
) -> list[str]:
    statuses = []
    t0 = time.time()
    n_lines = 0
    n_lines_timeseries = 0
    LOGGER.info(f"\nProcessing {country}")
    # Construct file paths
    output_paths = make_detail_and_summary_file_paths(country, indicator, export_directory)

    if os.path.exists(output_paths["output_detail_path"]):
        LOGGER.info(f"Detail file for {country}-{indicator} already exists")
        statuses.append(f"Output file {output_paths['output_detail_path']} already exists")
    else:
        # Make detail files
        LOGGER.info(f"Making detail file for {country}-{indicator}")
        try:
            country_dataframes = create_detail_dataframes(
                country, indicator, use_hdx_admin1=use_hdx_admin1
            )
            status = write_detail_data(country_dataframes, output_paths["output_detail_path"])
        except (Client.NoResult, AttributeError):
            country_dataframes = None
            status = f"There is no CLIMADA data for {country}-{indicator}"
        statuses.append(status)

    # Make summary file
    if country not in get_set_of_countries_in_summary_file(
        output_paths["output_summary_path"], indicator
    ):
        LOGGER.info(f"Making detail file for {country}-{indicator}")
        if country_dataframes is None:
            LOGGER.info(
                f"No country_dataframes available to make summary file for {country}-{indicator}"
            )
        else:
            summary_rows, n_lines = create_summary_data(country_dataframes)
            status = write_summary_data(summary_rows, output_paths["output_summary_path"])
            statuses.append(status)
    else:
        LOGGER.info(f"Summary data for {country}-{indicator} already exists")
        statuses.append(f"Output file {output_paths['output_detail_path']} already exists")

    # Make timeseries summary file
    if indicator in HAS_TIMESERIES:
        if country not in get_set_of_countries_in_summary_file(
            output_paths["output_timeseries_path"], indicator
        ):
            LOGGER.info(f"Making timeseries summary file for {country}-{indicator}")
            timeseries_summary_rows = []
            climada_properties = None
            if indicator == "river-flood":
                climada_properties = {"climate_scenario": "historical"}
            elif indicator == "tropical-cyclone":
                climada_properties = {"event_type": "observed"}

            try:
                timeseries_summary_rows = calculate_indicator_timeseries_admin(
                    country, indicator=indicator, climada_properties=climada_properties
                )
            except TypeError:
                LOGGER.info(
                    f".date attribute for {country}-{indicator} is malformed, "
                    "cannot make timeseries summary"
                )
            # For some reason Cameroon Flood throws AttributeError rather than Client.NoResult
            except (Client.NoResult, AttributeError):
                LOGGER.info(f"There is no CLIMADA data for {country}-{indicator}")

            if len(timeseries_summary_rows) != 0:
                n_lines_timeseries = len(timeseries_summary_rows)
                status = write_summary_data(
                    timeseries_summary_rows,
                    output_paths["output_timeseries_path"],
                    hxl_tags=TIMESERIES_HXL_TAGS,
                )
                statuses.append(status)
        else:
            LOGGER.info(f"Timeseries summary data for {country}-{indicator} already exists")

    statuses.append(
        f"Processing for {country} took {time.time()-t0:0.0f} seconds "
        f"and generated {n_lines} lines of summary output and "
        f"{n_lines_timeseries} lines of time series summary output"
    )

    return statuses


def make_detail_and_summary_file_paths(
    country: str, indicator: str, export_directory: str = None
) -> dict:
    if export_directory is None:
        export_directory = os.path.join(os.path.dirname(__file__), "output")

    file_path_dict = {}
    country_str = country.lower().replace(" ", "-")
    file_path_dict["output_detail_path"] = os.path.join(
        export_directory, f"{indicator}", f"{country_str}-admin1-{indicator}.csv"
    )

    file_path_dict["output_summary_path"] = os.path.join(
        export_directory, f"{indicator}", f"admin1-summaries-{indicator}.csv"
    )

    file_path_dict["output_timeseries_path"] = os.path.join(
        export_directory, f"{indicator}", f"admin1-timeseries-summaries-{indicator}.csv"
    )

    indicator_directory = os.path.dirname(file_path_dict["output_summary_path"])
    if not os.path.exists(indicator_directory):
        LOGGER.info(f"Creating {os.path.dirname(file_path_dict['output_summary_path'])}")
        os.makedirs(os.path.dirname(file_path_dict["output_summary_path"]), exist_ok=True)

    return file_path_dict


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

    # This chunk produces a list of dataframes from the supplied
    country_dataframes = []
    n_regions = len(admin1_shapes)
    for i, admin1_shape in enumerate(admin1_shapes, start=0):
        if admin1_names[i] is None:
            LOGGER.info("Admin1 name was 'None', continuing to next admin1")
        LOGGER.info(f"{i+1} of {n_regions} Processing {admin1_names[i]}")

        admin1_indicator_gdf = calculate_indicator_for_admin1(
            admin1_shape, admin1_names[i], country, indicator
        )

        LOGGER.info(f"Wrote {len(admin1_indicator_gdf)} lines")
        country_dataframes.append(admin1_indicator_gdf)

    return country_dataframes


def create_summary_data(
    country_dataframes: list[pd.DataFrame],
) -> tuple[list[dict], int]:
    summary_rows = []
    n_lines = 0

    for df in country_dataframes:
        if len(df) == 0:
            LOGGER.info("Dataframe length is zero")
            continue
        country = df["country_name"].unique()[0]
        indicators = df["indicator"].unique()
        for indicator in indicators:
            filtered_df = df[df["indicator"] == indicator]
            n_lines += len(filtered_df)
            row = HXL_TAGS.copy()
            row["country_name"] = country
            row["region_name"] = filtered_df["region_name"].to_list()[0]
            row["latitude"] = round(filtered_df["latitude"].mean(), 4)
            row["longitude"] = round(filtered_df["longitude"].mean(), 4)
            row["indicator"] = indicator
            value, aggregation = aggregate_value(indicator, filtered_df)
            row["value"] = value
            row["aggregation"] = aggregation

            LOGGER.info(f"{row['region_name']:<20}, " f"{indicator:<20}, " f"{row['value']:0.0f}")
            summary_rows.append(row)

    return summary_rows, n_lines


def write_summary_data(summary_rows: list, output_summary_path: str, hxl_tags: dict = None) -> str:
    if hxl_tags is None:
        hxl_tags = HXL_TAGS
    if not os.path.exists(output_summary_path):
        # This is slightly convoluted, but efficient
        # https://www.geeksforgeeks.org/python-perform-append-at-beginning-of-list/
        summary_rows = deque(summary_rows)
        summary_rows.appendleft(hxl_tags)
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
    country_indicator_gdf = pd.concat(country_dataframes, axis=0, ignore_index=True)
    hxl_tag_row = pd.DataFrame([HXL_TAGS])
    country_indicator_gdf = pd.concat(
        [hxl_tag_row, country_indicator_gdf], axis=0, ignore_index=True
    )
    country_indicator_gdf.to_csv(
        output_file_path,
        index=False,
    )

    status = f"Indicator data file written to {output_file_path}"

    return status


if __name__ == "__main__":
    COUNTRY = "Haiti"
    INDICATOR = "river-flood"
    LOGGER.info("Generating Climada csv files")
    LOGGER.info("============================")
    LOGGER.info(f"Timestamp: {datetime.datetime.now().isoformat()}")
    T0 = time.time()
    STATUSES = export_indicator_data_to_csv(country=COUNTRY, indicator=INDICATOR)
    for STATUS in STATUSES:
        LOGGER.info(STATUS)
    LOGGER.info(f"Processed all countries in {time.time()-T0:0.0f} seconds")
    LOGGER.info(f"Timestamp: {datetime.datetime.now().isoformat()}")
