#!/usr/bin/env python
# encoding: utf-8

import csv
import math
import pandas
import plotly.express as px

from hdx_scraper_climada.create_csv_files import make_detail_and_summary_file_paths
from hdx_scraper_climada.utilities import HAS_TIMESERIES


def calc_zoom(df: pandas.DataFrame) -> tuple[float, float]:
    """This function generates a zoom level to fit a supplied dataframe for display, cribbed from
    this stackoverflow answer:
    https://stackoverflow.com/questions/46891914/control-mapbox-extent-in-plotly-python-api

    Arguments:
        df {pandas.DataFrame} -- a Pandas dataframe containing latitude and longitude columns

    Returns:
        tuple[float, float] -- zoom for y, zoom for x
    """
    width_y = max(df["latitude"]) - min(df["latitude"])
    width_x = max(df["longitude"]) - min(df["longitude"])
    zoom_y = -1.446 * math.log(width_y) + 7.2753
    zoom_x = -1.415 * math.log(width_x) + 8.7068
    return min(round(zoom_y, 2), round(zoom_x, 2))


def plot_detail_file_map(country: str, indicator: str):
    country_data = get_data_from_csv(country, indicator)
    if country_data is None:
        return None
    if indicator == "litpop":
        country_data["scaled_value"] = 0.1 + 10.0 * country_data["value"] / max(
            country_data["value"]
        )
    elif indicator == "earthquake":
        country_data["scaled_value"] = (country_data["value"] - min(country_data["value"])) * 5.0
    elif indicator == "flood":
        print(f"Size of flood data before scaling: {len(country_data)}", flush=True)
        country_data["scaled_value"] = country_data["value"] * 10.0
        country_data = country_data[country_data["value"] != 0]
        print(f"Size of flood data after scaling: {len(country_data)}", flush=True)
    zoom = calc_zoom(country_data)
    print(f"Calculated zoom: {zoom}", flush=True)
    fig = px.scatter_mapbox(
        country_data,
        lat="latitude",
        lon="longitude",
        color="region_name",
        size="scaled_value",
        hover_data="value",
        title=f"{indicator} data for {country}",
        size_max=20,
        zoom=zoom,
        opacity=0.75,
        mapbox_style="carto-darkmatter",
        width=1200,
        height=800,
    )
    fig.show()


def plot_histogram(country: str, indicator: str):
    country_data = get_data_from_csv(country, indicator)
    fig = px.histogram(country_data, x="value", title=f"{indicator} histogram for {country}")
    fig.show()


def plot_timeseries_histogram(country: str, indicator: str):
    if indicator not in HAS_TIMESERIES:
        print(f"Indicator '{indicator}' does not have time series data")
    output_paths = make_detail_and_summary_file_paths(country, indicator)

    timeseries_data = pandas.read_csv(output_paths["output_timeseries_path"])

    fig = px.bar(timeseries_data, x="event_date")
    fig.show()
    # with open("admin1-timeseries-summaries.csv", encoding="utf-8") as summary_file:
    #     summary_data = list(csv.DictReader(summary_file))

    # date_set = set(x["event_date"] for x in summary_data)


def plot_timeseries_chloropleth(country: str, indicator: str):
    print(date_set, flush=True)
    values = {x["admin2_name"]: x["value"] for x in summary_data if x["event_date"] == "2010-01-13"}
    value_column = [float(values.get(x, 0.0)) for x in admin2_names]
    all_shapes = geopandas.GeoDataFrame(pandas.concat(admin2_shapes, ignore_index=True))
    all_shapes["admin1_names"] = admin1_names
    all_shapes["admin2_names"] = admin2_names
    all_shapes["value"] = value_column
    all_shapes.plot(column="value", cmap="Spectral", legend=True)


def get_data_from_csv(country: str, indicator: str) -> pandas.DataFrame | None:
    output_paths = make_detail_and_summary_file_paths(country, indicator)
    country_data = pandas.read_csv(output_paths["output_detail_path"])

    if country == "Syrian Arab Republic" and indicator == "litpop":
        print("No 'litpop' data for Syrian Arab Republic", flush=True)
        return None

    country_data.drop(country_data.head(1).index, inplace=True)
    country_data["latitude"] = country_data["latitude"].astype(float)
    country_data["longitude"] = country_data["longitude"].astype(float)
    country_data["value"] = country_data["value"].astype(float)
    return country_data
