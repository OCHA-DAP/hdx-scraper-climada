#!/usr/bin/env python
# encoding: utf-8

import csv
import math
import geopandas
import pandas
import plotly.express as px

from hdx.location.country import Country

from hdx_scraper_climada.create_csv_files import make_detail_and_summary_file_paths
from hdx_scraper_climada.utilities import HAS_TIMESERIES
from hdx_scraper_climada.download_admin1_geometry import (
    get_best_admin_shapes,
)

INDICATOR_UNITS = {"earthquake": "Maximum MMI", "flood": "extent", "litpop": "USD"}


def calc_zoom(df: pandas.DataFrame, total_bounds: None | list = None) -> tuple[float, float]:
    """This function generates a zoom level to fit a supplied dataframe for display, cribbed from
    this stackoverflow answer:
    https://stackoverflow.com/questions/46891914/control-mapbox-extent-in-plotly-python-api

    Arguments:
        df {pandas.DataFrame} -- a Pandas dataframe containing latitude and longitude columns

    Returns:
        tuple[float, float] -- zoom for y, zoom for x
    """
    if total_bounds is not None:
        min_latitude = total_bounds[1]
        max_latitude = total_bounds[3]
        min_longitude = total_bounds[0]
        max_longitude = total_bounds[2]
    else:
        min_latitude = min(df["latitude"])
        max_latitude = max(df["latitude"])
        min_longitude = min(df["longitude"])
        max_longitude = max(df["longitude"])

    width_y = max_latitude - min_latitude
    width_x = max_longitude - min_longitude
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
    fig = px.scatter_mapbox(
        country_data,
        lat="latitude",
        lon="longitude",
        color="region_name",
        size="scaled_value",
        hover_data="value",
        title=(
            f"Gridded {indicator.title()} data for {country}, " "sized by value, coloured by admin1"
        ),
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
    if country_data is None:
        print(f"No {indicator} data for {country}", flush=True)
        return None
    fig = px.histogram(
        country_data,
        x="value",
        title=(
            f"{indicator.title()} {INDICATOR_UNITS[indicator]} "
            f"histogram for {country} from detail data"
        ),
    )
    fig.update_layout(xaxis_title_text=f"{INDICATOR_UNITS[indicator]}", yaxis_title_text="Count")
    fig.show()


def plot_timeseries_histogram(country: str, indicator: str):
    if indicator not in HAS_TIMESERIES:
        print(f"plot_timeseries_histogram: Indicator '{indicator}' does not have time series data")
        return None
    output_paths = make_detail_and_summary_file_paths(country, indicator)
    timeseries_data = pandas.read_csv(output_paths["output_timeseries_path"])

    timeseries_data.drop(timeseries_data.head(1).index, inplace=True)
    timeseries_data = timeseries_data[timeseries_data["country_name"] == country]
    if len(timeseries_data) == 0:
        print(f"No {indicator} timeseries data for {country}", flush=True)
        return None
    timeseries_data["value"] = timeseries_data["value"].astype(float)
    timeseries_data["event_date"] = pandas.to_datetime(timeseries_data["event_date"])

    date_set = sorted(timeseries_data["event_date"].unique())

    for i, date_ in enumerate(date_set):
        print(f"{i}. {date_}", flush=True)

    resampled_intensity = (
        timeseries_data.reset_index().resample("Y", on="event_date").max()["value"]
    )

    display_data = pandas.DataFrame()
    display_data["event_date"] = resampled_intensity.index
    display_data["value"] = resampled_intensity.to_list()

    fig = px.bar(
        display_data,
        x="event_date",
        y="value",
        title=f"{indicator.title()} {INDICATOR_UNITS[indicator]} for {country} resampled to yearly",
    )
    fig.update_layout(
        xaxis_title_text="Year",
        yaxis_title_text=f"{INDICATOR_UNITS[indicator]}",
        xaxis={"tickmode": "linear", "tick0": "2000-06-30", "dtick": "M60", "tickformat": "%Y"},
    )
    fig.show()


def plot_admin_boundaries(country: str):
    country_iso3alpha = Country.get_iso3_country_code(country)
    admin1_names, admin2_names, admin_shapes, admin_level = get_best_admin_shapes(country_iso3alpha)

    admin_name_column = f"admin{admin_level}_names"

    all_shapes = geopandas.GeoDataFrame(pandas.concat(admin_shapes, ignore_index=True))
    all_shapes["admin1_names"] = admin1_names
    all_shapes["admin2_names"] = admin2_names

    total_bounds = all_shapes.total_bounds
    center_lat = total_bounds[1] + (total_bounds[3] - total_bounds[1]) / 2.0
    center_lon = total_bounds[0] + (total_bounds[2] - total_bounds[0]) / 2.0
    zoom = calc_zoom(None, total_bounds=total_bounds)
    # all_shapes.plot(column=admin_name_column, categorical=True, cmap="Spectral")
    fig = px.choropleth_mapbox(
        all_shapes,
        geojson=all_shapes.geometry,
        locations=all_shapes.index,
        center={"lat": center_lat, "lon": center_lon},
        zoom=zoom,
        color=admin_name_column,
        title=f"{len(admin2_names)} admin{admin_level} boundaries for {country}",
        hover_data=["admin1_names", "admin2_names"],
        mapbox_style="carto-darkmatter",
        opacity=0.75,
        width=1200,
        height=800,
    )
    # fig.update_geos(fitbounds="locations", visible=False)

    fig.show()


def plot_timeseries_chloropleth(country: str, indicator: str, event_idx: None | int = None):
    if indicator not in HAS_TIMESERIES:
        print(
            f"plot_timeseries_chloropleth: Indicator '{indicator}' does not have time series data"
        )
        return None
    country_iso3alpha = Country.get_iso3_country_code(country)
    admin1_names, admin2_names, admin_shapes, admin_level = get_best_admin_shapes(country_iso3alpha)
    admin_column_name = f"admin{admin_level}_name"
    output_paths = make_detail_and_summary_file_paths(country, indicator)
    with open(output_paths["output_timeseries_path"], encoding="utf-8") as timeseries_file:
        timeseries_data = list(csv.DictReader(timeseries_file))

    timeseries_data = timeseries_data[1:]
    timeseries_data = [x for x in timeseries_data if x["country_name"] == country]

    if len(timeseries_data) == 0:
        print(f"No {indicator} timeseries data for {country}", flush=True)
        return None

    date_set = set(x["event_date"] for x in timeseries_data)
    date_list = sorted(list(date_set))
    if event_idx is None:
        maximum_intensity_record = max(timeseries_data, key=lambda x: x["value"])
        event_date = maximum_intensity_record["event_date"]

    else:
        try:
            event_date = date_list[event_idx]
        except IndexError:
            event_date = date_list[0]

    print(f"Selected date {event_date} from a list of length {len(date_list)}", flush=True)

    values = {
        x[admin_column_name]: x["value"] for x in timeseries_data if x["event_date"] == event_date
    }

    all_shapes = geopandas.GeoDataFrame(pandas.concat(admin_shapes, ignore_index=True))
    all_shapes["admin1_name"] = admin1_names
    all_shapes["admin2_name"] = admin2_names
    value_column = [float(values.get(x, 0.0)) for x in all_shapes[admin_column_name]]
    all_shapes["value"] = value_column

    # all_shapes.plot(column="value", cmap="Spectral", legend=True)

    total_bounds = all_shapes.total_bounds
    center_lat = total_bounds[1] + (total_bounds[3] - total_bounds[1]) / 2.0
    center_lon = total_bounds[0] + (total_bounds[2] - total_bounds[0]) / 2.0
    zoom = calc_zoom(None, total_bounds=total_bounds)
    # all_shapes.plot(column=admin_name_column, categorical=True, cmap="Spectral")
    fig = px.choropleth_mapbox(
        all_shapes,
        geojson=all_shapes.geometry,
        locations=all_shapes.index,
        center={"lat": center_lat, "lon": center_lon},
        title=(
            f"{indicator.title()} {INDICATOR_UNITS[indicator]} data for {country} "
            f"for an event at {event_date}"
        ),
        zoom=zoom,
        color="value",
        color_continuous_scale=px.colors.sequential.Jet,
        hover_data=["value", admin_column_name],
        mapbox_style="carto-darkmatter",
        opacity=0.75,
        width=1200,
        height=800,
    )
    # fig.update_geos(fitbounds="locations", visible=False)

    fig.show()


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
