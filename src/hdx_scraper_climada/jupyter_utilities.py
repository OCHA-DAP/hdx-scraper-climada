#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import math
import os

import geopandas
import pandas
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from hdx.location.country import Country

from hdx_scraper_climada.create_csv_files import make_detail_and_summary_file_paths
from hdx_scraper_climada.utilities import HAS_TIMESERIES, read_countries
from hdx_scraper_climada.download_from_hdx import (
    get_best_admin_shapes,
)

INDICATOR_UNITS = {
    "litpop": "USD",
    "crop-production": "USD",
    "earthquake": "Maximum MMI",
    "flood": "Extent (200m grid points)",
    "wildfire": "Extent (4km grid points)",
    "river-flood": "Extent (4km grid points)",
    "tropical-cyclone": "Maximum wind speed (m/s)",
    "storm-europe": "Maximum wind speed (m/s)",
}


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


def plot_summary_barcharts(country: str, indicator: str, export_directory: str = None):
    display_data = get_summary_data_from_csv(country, indicator, export_directory=export_directory)

    if country is not None:
        country_data = display_data[display_data["country_name"] == country]

        fig = px.bar(
            country_data,
            x="admin1_name",
            y="value",
            title=f"Summary {indicator} data for {country}",
        )
        fig.update_layout(
            xaxis_title_text="Admin1 name", yaxis_title_text=f"{INDICATOR_UNITS[indicator]}"
        )
        fig.show()
    else:
        countries = read_countries(indicator=indicator)
        countries_ = [x["country_name"] for x in countries]
        n_cols = 5
        n_rows = 5
        print(f"Number of countries for indicator '{indicator}' is '{len(countries_)}'")
        if len(countries_) == 35:
            n_rows = 7
        elif len(countries_) == 36:
            n_rows = 6
            n_cols = 6

        fig = make_subplots(
            rows=n_rows,
            cols=n_cols,
            specs=[[{"type": "bar"}] * n_cols] * n_rows,
            subplot_titles=countries_,
            vertical_spacing=0.1,
        )

        figlets = []
        for country_ in countries_:
            country_data = display_data[display_data["country_name"] == country_]
            figlets.append(
                go.Bar(
                    x=country_data["admin1_name"],
                    y=country_data["value"],
                )
            )

        for i, figlet in enumerate(figlets, start=1):
            row = int((i - 1) / n_cols) + 1
            col = int((i - 1) % n_cols) + 1
            fig.add_trace(figlet, row=row, col=col)

        fig.update_layout(
            height=1800,
            width=1200,
            showlegend=False,
            title=(
                f"Summary {indicator} data ({INDICATOR_UNITS[indicator]}) by admin1 "
                f"for all HRP countries"
            ),
        )
        fig.show()

        image_file_path = make_image_file_path(indicator, "all-country", "barcharts")
        fig.write_image(image_file_path)


def make_image_file_path(indicator: str, country: str, identifier: str) -> str:
    isodate = datetime.datetime.now().isoformat()[0:10]
    image_file_path = os.path.join(
        os.path.dirname(__file__),
        "analysis",
        f"{isodate}-{indicator}-{country}-{identifier}.png",
    )

    return image_file_path


def plot_detail_file_map(country: str, indicator: str, export_directory: str = None):
    country_data = get_detail_data_from_csv(country, indicator, export_directory=export_directory)
    if country_data is None:
        return None
    if indicator == "litpop":
        country_data["scaled_value"] = 0.1 + 10.0 * country_data["value"] / max(
            country_data["value"]
        )
    elif indicator == "crop-production":
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
    elif indicator == "wildfire":
        country_data = country_data[country_data["value"] != 0.0]
        country_data["scaled_value"] = (country_data["value"] - 293.0) / 10.0
    elif indicator == "river-flood":
        country_data = country_data[country_data["value"] != 0.0]
        country_data["scaled_value"] = country_data["value"]
    elif indicator == "tropical-cyclone":
        country_data["scaled_value"] = country_data["value"] - min(country_data["value"])
    elif indicator == "storm-europe":
        country_data["scaled_value"] = country_data["value"] - min(country_data["value"])
    zoom = calc_zoom(country_data)
    fig = px.scatter_mapbox(
        country_data,
        lat="latitude",
        lon="longitude",
        color="admin1_name",
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
    image_file_path = make_image_file_path(indicator, country, "gridded")
    fig.write_image(image_file_path)

    return None


def plot_histogram(country: str, indicator: str, export_directory: str = None):
    country_data = get_detail_data_from_csv(country, indicator, export_directory=export_directory)
    if country_data is None:
        print(f"No {indicator} data for {country}", flush=True)
        return None

    country_data = country_data[country_data["value"] != 0]
    fig = px.histogram(
        country_data,
        x="value",
        title=(
            f"{indicator.title()} {INDICATOR_UNITS[indicator]} "
            f"histogram for {country} from detail data"
        ),
    )
    fig.update_layout(
        xaxis_title_text=f"{INDICATOR_UNITS[indicator]}", yaxis_title_text="Count", width=1200
    )
    fig.show()
    image_file_path = make_image_file_path(indicator, country, "histogram")
    fig.write_image(image_file_path)
    return None


def plot_timeseries_histogram(country: str, indicator: str, export_directory: str = None):
    if indicator not in HAS_TIMESERIES:
        print(f"plot_timeseries_histogram: Indicator '{indicator}' does not have time series data")
        return None
    timeseries_data = get_timeseries_data_from_csv(
        country, indicator, export_directory=export_directory
    )
    timeseries_data = timeseries_data[timeseries_data["country_name"] == country]
    if len(timeseries_data) == 0:
        print(f"No {indicator} timeseries data for {country}", flush=True)
        return None
    timeseries_data["value"] = timeseries_data["value"].astype(float)
    timeseries_data["event_date"] = pandas.to_datetime(timeseries_data["event_date"])

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
        width=1200,
    )
    fig.show()

    image_file_path = make_image_file_path(indicator, country, "event-histogram")
    fig.write_image(image_file_path)
    return None


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


def plot_timeseries_chloropleth(
    country: str, indicator: str, event_idx: None | int = None, export_directory: str = None
):
    if indicator not in HAS_TIMESERIES:
        print(
            f"plot_timeseries_chloropleth: Indicator '{indicator}' does not have time series data"
        )
        return None
    country_iso3alpha = Country.get_iso3_country_code(country)
    admin1_names, admin2_names, admin_shapes, admin_level = get_best_admin_shapes(country_iso3alpha)
    admin_column_name = f"admin{admin_level}_name"
    output_paths = make_detail_and_summary_file_paths(
        country, indicator, export_directory=export_directory
    )
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

    image_file_path = make_image_file_path(indicator, country, "event-chloropleth")
    fig.write_image(image_file_path)

    return None


def get_detail_data_from_csv(
    country: str, indicator: str, export_directory: str = None
) -> pandas.DataFrame | None:
    output_paths = make_detail_and_summary_file_paths(
        country, indicator, export_directory=export_directory
    )
    country_data = pandas.read_csv(output_paths["output_detail_path"])

    if country == "Syrian Arab Republic" and indicator == "litpop":
        print("No 'litpop' data for Syrian Arab Republic", flush=True)
        return None

    country_data.drop(country_data.head(1).index, inplace=True)

    if "region_name" in country_data:
        country_data.rename(columns={"region_name": "admin1_name"}, inplace=True)
    country_data["latitude"] = country_data["latitude"].astype(float)
    country_data["longitude"] = country_data["longitude"].astype(float)
    country_data["value"] = country_data["value"].astype(float)
    return country_data


def get_summary_data_from_csv(
    country: str, indicator: str, export_directory: str = None
) -> pandas.DataFrame | None:
    if country is None:
        country = "Haiti"
    output_paths = make_detail_and_summary_file_paths(
        country, indicator, export_directory=export_directory
    )
    country_data = pandas.read_csv(output_paths["output_summary_path"])

    country_data.drop(country_data.head(1).index, inplace=True)
    if "region_name" in country_data:
        country_data.rename(columns={"region_name": "admin1_name"}, inplace=True)
    country_data["latitude"] = country_data["latitude"].astype(float)
    country_data["longitude"] = country_data["longitude"].astype(float)
    country_data["value"] = country_data["value"].astype(float)
    return country_data


def get_timeseries_data_from_csv(
    country: str, indicator: str, export_directory: str = None
) -> pandas.DataFrame:
    output_paths = make_detail_and_summary_file_paths(
        country, indicator, export_directory=export_directory
    )
    timeseries_data = pandas.read_csv(output_paths["output_timeseries_path"])

    timeseries_data.drop(timeseries_data.head(1).index, inplace=True)
    if "region_name" in timeseries_data:
        timeseries_data.rename(columns={"region_name": "admin1_name"}, inplace=True)
    timeseries_data["latitude"] = timeseries_data["latitude"].astype(float)
    timeseries_data["longitude"] = timeseries_data["longitude"].astype(float)
    timeseries_data["value"] = timeseries_data["value"].astype(float)
    return timeseries_data
