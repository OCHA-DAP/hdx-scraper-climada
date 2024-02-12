#!/usr/bin/env python
# encoding: utf-8

import math
import pandas
import plotly.express as px

from hdx_scraper_climada.create_csv_files import make_detail_and_summary_file_paths


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
    output_paths = make_detail_and_summary_file_paths(country, indicator)
    country_data = pandas.read_csv(output_paths["output_detail_path"])

    if country == "Syrian Arab Republic" and indicator == "litpop":
        print("No 'litpop' data for Syrian Arab Republic", flush=True)
        return

    country_data.drop(country_data.head(1).index, inplace=True)
    country_data["latitude"] = country_data["latitude"].astype(float)
    country_data["longitude"] = country_data["longitude"].astype(float)
    country_data["value"] = country_data["value"].astype(float)
    country_data["value"] = 0.1 + 10.0 * country_data["value"] / max(country_data["value"])
    zoom = calc_zoom(country_data)
    print(f"Calculated zoom: {zoom}", flush=True)
    fig = px.scatter_mapbox(
        country_data,
        lat="latitude",
        lon="longitude",
        color="region_name",
        size="value",
        title=f"{indicator} data for {country}",
        size_max=20,
        zoom=zoom,
        opacity=0.75,
        mapbox_style="carto-darkmatter",
        width=1200,
        height=800,
    )
    fig.show()
