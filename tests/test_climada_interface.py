#!/usr/bin/env python
# encoding: utf-8

"""
This test suite tests aspects of the climada interface for issue reporting
Ian Hopkinson 2024-01-16
"""

import os
import time
import pandas as pd
import pytest


from climada.util.api_client import Client

# from climada.entity.exposures import LitPop

from hdx_scraper_climada.patched_litpop import LitPop
from hdx_scraper_climada.download_from_hdx import (
    get_admin1_shapes_from_hdx,
    get_admin2_shapes_from_hdx,
)

from hdx_scraper_climada.climada_interface import (
    calculate_indicator_for_admin1,
    calculate_indicator_timeseries_admin,
    filter_dataframe_with_geometry,
    flood_timeseries_data_shim,
    get_date_range_from_live_api,
)

from hdx_scraper_climada.create_csv_files import make_detail_and_summary_file_paths


COUNTRY_ISO3A = "HTI"
COUNTRY = "Haiti"
ADMIN1_NAMES, ADMIN1_SHAPES = get_admin1_shapes_from_hdx(COUNTRY_ISO3A)


@pytest.mark.local_only
def test_afghanistan_litpop():
    country_iso3a = "AFG"
    afghanistan_litpop = LitPop.from_countries(country_iso3a)
    afghanistan_litpop_gdf = afghanistan_litpop.gdf

    assert not afghanistan_litpop_gdf["value"].isna().any()


@pytest.mark.local_only
def test_syria_litpop():
    country_iso3a = "SYR"
    syria_litpop = LitPop.from_countries(country_iso3a)
    syria_litpop_gdf = syria_litpop.gdf

    assert syria_litpop_gdf["value"].isna().sum() == len(syria_litpop_gdf)


@pytest.mark.local_only
def test_syria_litpop_nightlight_intensity():
    country_iso3a = "SYR"
    syria_litpop = LitPop.from_nightlight_intensity(country_iso3a)
    syria_litpop_gdf = syria_litpop.gdf

    assert syria_litpop_gdf["value"].isna().sum() == 0


@pytest.mark.local_only
def test_syria_litpop_population():
    country_iso3a = "SYR"
    syria_litpop = LitPop.from_population(country_iso3a)
    syria_litpop_gdf = syria_litpop.gdf

    assert syria_litpop_gdf["value"].isna().sum() == 0


# hopefully fixed by vendoring the black marble files
# @pytest.mark.skip(reason="Failing because of Black Marble issues")
def test_calculate_indicator_for_admin1_litpop():
    indicator = "litpop"

    admin1_indicator_gdf = calculate_indicator_for_admin1(
        ADMIN1_SHAPES[0], ADMIN1_NAMES[0], COUNTRY, indicator
    )

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "admin1_name": "Centre",
        "latitude": 19.3125,
        "longitude": -72.02083333,
        "aggregation": "none",
        "indicator": "litpop",
        "value": 759342.0,
    }

    assert len(admin1_indicator_gdf) == 176


@pytest.mark.local_only
def test_calculate_indicator_for_admin1_litpop_alt():
    indicator = "litpop_alt"

    admin1_indicator_gdf = calculate_indicator_for_admin1(
        ADMIN1_SHAPES[0], ADMIN1_NAMES[0], COUNTRY, indicator
    )

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "admin1_name": "Centre",
        "latitude": 19.3125,
        "longitude": -72.02083333,
        "aggregation": "none",
        "indicator": "litpop_alt",
        "value": 852594.0,
    }

    assert len(admin1_indicator_gdf) == 176


@pytest.mark.local_only
def test_litpop_cross_check():
    # This test shows that the litpop value from the Litpop Class and the litpop value from
    # the get_exposures method differ consistently by about 10%. This is likely an issue
    # with the GPW population statistics. It demonstrates that the region extraction code is
    # consistent between the two methods.
    admin1_litpop_gdf = calculate_indicator_for_admin1(
        ADMIN1_SHAPES[0], ADMIN1_NAMES[0], COUNTRY, "litpop"
    )
    admin1_litpop_alt_gdf = calculate_indicator_for_admin1(
        ADMIN1_SHAPES[0], ADMIN1_NAMES[0], COUNTRY, "litpop_alt"
    )

    assert len(admin1_litpop_gdf) == len(admin1_litpop_alt_gdf)

    for i in range(0, len(admin1_litpop_gdf)):
        litpop_row = admin1_litpop_gdf.iloc[i].to_dict()
        litpop_alt_row = admin1_litpop_alt_gdf.iloc[i].to_dict()
        assert litpop_row["country_name"] == "Haiti"
        assert litpop_alt_row["country_name"] == "Haiti"
        assert litpop_row["admin1_name"] == "Centre"
        assert litpop_alt_row["admin1_name"] == "Centre"
        assert litpop_row["indicator"] == "litpop"
        assert litpop_alt_row["indicator"] == "litpop_alt"
        assert litpop_row["latitude"] == litpop_alt_row["latitude"]
        assert litpop_row["longitude"] == litpop_alt_row["longitude"]

        assert litpop_row["value"] / litpop_alt_row["value"] == pytest.approx(0.89062, abs=0.00002)


def test_calculate_indicator_for_admin1_crop_production():
    indicator = "crop-production"

    admin1_indicator_gdf_list = []
    for i, admin1_shape in enumerate(ADMIN1_SHAPES):
        admin1_indicator_gdf_list.append(
            calculate_indicator_for_admin1(admin1_shape, ADMIN1_NAMES[i], COUNTRY, indicator)
        )

    admin1_indicator_gdf = pd.concat(admin1_indicator_gdf_list)

    export_directory = os.path.join(os.path.dirname(__file__), "temp")
    output_paths = make_detail_and_summary_file_paths(
        COUNTRY, indicator, export_directory=export_directory
    )
    admin1_indicator_gdf.to_csv(output_paths["output_detail_path"], index=False)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "admin1_name": "Centre",
        "latitude": 19.25,
        "longitude": -71.75,
        "aggregation": "none",
        "indicator": "crop-production.mai.noirr.USD",
        "value": 4550041.0,
    }

    assert len(admin1_indicator_gdf) == 128


def test_calculate_indicator_for_admin1_earthquake():
    indicator = "earthquake"

    admin1_indicator_gdf_list = []
    for i, admin1_shape in enumerate(ADMIN1_SHAPES):
        admin1_indicator_gdf_list.append(
            calculate_indicator_for_admin1(admin1_shape, ADMIN1_NAMES[i], COUNTRY, indicator)
        )

    admin1_indicator_gdf = pd.concat(admin1_indicator_gdf_list)

    export_directory = os.path.join(os.path.dirname(__file__), "temp")
    output_paths = make_detail_and_summary_file_paths(
        COUNTRY, indicator, export_directory=export_directory
    )
    admin1_indicator_gdf.to_csv(output_paths["output_detail_path"], index=False)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "admin1_name": "Centre",
        "latitude": 19.29167,
        "longitude": -72.20833,
        "aggregation": "none",
        "indicator": "earthquake",
        "value": 6.67,
    }

    assert len(admin1_indicator_gdf) == 1300


def test_calculate_indicator_timeseries_admin():
    country = "Haiti"
    earthquakes = calculate_indicator_timeseries_admin(
        country, indicator="earthquake", test_run=True
    )

    assert len(earthquakes) == 35
    assert earthquakes[0] == {
        "country_name": "Haiti",
        "admin1_name": "South",
        "admin2_name": "Les Cayes",
        "latitude": 18.2625,
        "longitude": -73.7667,
        "aggregation": "max",
        "indicator": "earthquake.date",
        "event_date": "1907-01-14T00:00:00",
        "value": 4.22,
    }


@pytest.mark.skip(reason="Runtime over 10 minutes - currently failing")
def test_calculate_indicator_timeseries_admin_storm_europe():
    country = "Ukraine"
    storm_europe = calculate_indicator_timeseries_admin(
        country, indicator="storm_europe", test_run=False
    )

    assert len(storm_europe) == 1
    assert storm_europe[0] == {
        "country_name": "Ukraine",
        "admin1_name": "Odeska",
        "admin2_name": "",
        "latitude": 46.7483,
        "longitude": -73.7667,
        "aggregation": "max",
        "indicator": "earthquake.date",
        "event_date": "1913-06-14T00:00:00",
        "value": 4.65,
    }


def test_filter_dataframe_with_geometry():
    t0 = time.time()
    admin1_names, admin2_names, admin_shapes = get_admin2_shapes_from_hdx(COUNTRY_ISO3A)
    print(f"{time.time() - t0:0.2f} seconds to load admin2 shapes")

    t0 = time.time()
    client = Client()
    print(f"{time.time() - t0:0.2f} seconds to create API client")

    t0 = time.time()
    earthquake = client.get_hazard(
        "earthquake",
        properties={
            "country_iso3alpha": "HTI",
        },
    )
    print(f"{time.time() - t0:0.2f} seconds to get earthquake data")
    indicator_key = "test"
    latitudes = earthquake.centroids.lat
    longitudes = earthquake.centroids.lon

    non_zero_intensity = earthquake.intensity[107]
    values = non_zero_intensity.toarray().flatten()

    country_data = pd.DataFrame(
        {
            "latitude": latitudes,
            "longitude": longitudes,
            "value": values,
        }
    )

    t0 = time.time()
    # This run populates the cache - we prepend "test" to the cache key to make sure we haven't
    # accidently warmed the cache
    for j, admin_shape in enumerate(admin_shapes):
        cache_key = f"test-{admin1_names[j]}-{admin2_names[j]}"
        admin_indicator_uncached_gdf = filter_dataframe_with_geometry(
            country_data, admin_shape, indicator_key, cache_key=cache_key
        )

    uncached_time = time.time() - t0
    print(f"{time.time() - t0:0.2f} seconds to on filter {len(admin_shapes)} shape")

    t0 = time.time()
    # This run uses the cache
    for j, admin_shape in enumerate(admin_shapes):
        cache_key = f"test-{admin1_names[j]}-{admin2_names[j]}"
        admin_indicator_cached_gdf = filter_dataframe_with_geometry(
            country_data, admin_shape, indicator_key, cache_key=cache_key
        )
    cached_time = time.time() - t0
    print(f"{time.time() - t0:0.2f} seconds to on filter {len(admin_shapes)} shape with caching")

    # This tests fails intermittently - the speed up provided by the cache varies a bit from run to
    # run.
    # assert cached_time / uncached_time < 0.90
    print(f"cached/uncached time ratio = {cached_time / uncached_time:0.2f} - expected 0.9")
    assert admin_indicator_uncached_gdf["value"].equals(admin_indicator_cached_gdf["value"])


def test_calculate_indicator_for_admin1_flood():
    indicator = "flood"

    admin1_indicator_gdf_list = []
    for i, admin1_shape in enumerate(ADMIN1_SHAPES):
        admin1_indicator_gdf_list.append(
            calculate_indicator_for_admin1(admin1_shape, ADMIN1_NAMES[i], COUNTRY, indicator)
        )

    admin1_indicator_gdf = pd.concat(admin1_indicator_gdf_list)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "admin1_name": "Centre",
        "latitude": 19.21609,
        "longitude": -71.69117,
        "aggregation": "none",
        "indicator": "flood",
        "value": 1.0,
    }

    # We expect this figure to be higher than for earthquake since it is on a 200mx200m grid
    # rather than a 4kmx4km grid even though zero values are filtered out
    assert len(admin1_indicator_gdf) == 4618


def test_calculate_indicator_for_admin1_wildfire():
    indicator = "wildfire"

    admin1_indicator_gdf_list = []
    for i, admin1_shape in enumerate(ADMIN1_SHAPES):
        admin1_indicator_gdf_list.append(
            calculate_indicator_for_admin1(admin1_shape, ADMIN1_NAMES[i], COUNTRY, indicator)
        )

    admin1_indicator_gdf = pd.concat(admin1_indicator_gdf_list)

    # export_directory = os.path.join(os.path.dirname(__file__), "temp")
    # output_paths = make_detail_and_summary_file_paths(
    #     COUNTRY, indicator, export_directory=export_directory
    # )
    # admin1_indicator_gdf.to_csv(output_paths["output_detail_path"], index=False)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "admin1_name": "Centre",
        "latitude": 19.29167,
        "longitude": -72.20833,
        "aggregation": "none",
        "indicator": "wildfire",
        "value": 341.0,
    }

    assert len(admin1_indicator_gdf) == 1300


@pytest.mark.skip(reason="Causing OOM Locally and failure on GitHub Actions")
def test_flood_shim():
    # This shim takes event date information from a file and puts it into a Hazard object to
    # replace malformed event date. Described in this issue on the CLIMADA repo
    # https://github.com/CLIMADA-project/climada_python/issues/850
    # The countries effected are Colombia, Nigeria, Sudan and Venezuela
    # The lookup is from a dfo event number to an ordinal date
    client = Client()
    flood_data = client.get_hazard(
        "flood",
        properties={
            "country_name": "Colombia",
        },
    )

    flood_data = flood_timeseries_data_shim(flood_data)

    assert flood_data.date == [
        731988,
        735649,
        731905,
        736545,
        734624,
        735982,
        731740,
        732671,
        732392,
        733071,
        732815,
        733076,
        733389,
        733189,
        734872,
        734450,
        735375,
        732950,
        733450,
        734091,
        734101,
        732204,
        732336,
        734563,
        732358,
        731036,
    ]


def test_calculate_indicator_for_admin1_river_flood():
    indicator = "river-flood"

    admin1_indicator_gdf_list = []
    for i, admin1_shape in enumerate(ADMIN1_SHAPES):
        admin1_indicator_gdf_list.append(
            calculate_indicator_for_admin1(admin1_shape, ADMIN1_NAMES[i], COUNTRY, indicator)
        )

    admin1_indicator_gdf = pd.concat(admin1_indicator_gdf_list)

    export_directory = os.path.join(os.path.dirname(__file__), "temp")
    output_paths = make_detail_and_summary_file_paths(
        COUNTRY, indicator, export_directory=export_directory
    )
    admin1_indicator_gdf.to_csv(output_paths["output_detail_path"], index=False)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "admin1_name": "Centre",
        "latitude": 19.29167,
        "longitude": -72.20833,
        "aggregation": "none",
        "indicator": "river-flood",
        "value": 0.0,
    }

    assert len(admin1_indicator_gdf) == 1300


def test_calculate_indicator_for_admin1_tropical_cyclone():
    indicator = "tropical-cyclone"

    admin1_indicator_gdf_list = []
    for i, admin1_shape in enumerate(ADMIN1_SHAPES):
        admin1_indicator_gdf_list.append(
            calculate_indicator_for_admin1(admin1_shape, ADMIN1_NAMES[i], COUNTRY, indicator)
        )

    admin1_indicator_gdf = pd.concat(admin1_indicator_gdf_list)

    export_directory = os.path.join(os.path.dirname(__file__), "temp")
    output_paths = make_detail_and_summary_file_paths(
        COUNTRY, indicator, export_directory=export_directory
    )
    admin1_indicator_gdf.to_csv(output_paths["output_detail_path"], index=False)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Haiti",
        "admin1_name": "Centre",
        "latitude": 19.29167,
        "longitude": -72.20833,
        "aggregation": "none",
        "indicator": "tropical-cyclone",
        "value": 34.0,
    }

    assert len(admin1_indicator_gdf) == 1300


@pytest.mark.skip(reason="Runtime over 10 minutes")
def test_calculate_indicator_for_admin1_storm_europe():
    # Storm-europe is unusual, of the HRP countries it is only available for Ukraine
    country = "Ukraine"
    indicator = "storm-europe"
    ukr_admin1_names, ukr_admin1_shapes = get_admin1_shapes_from_hdx("UKR")

    admin1_indicator_gdf_list = []
    for i, ukr_admin1_shape in enumerate(ukr_admin1_shapes):
        admin1_indicator_gdf_list.append(
            calculate_indicator_for_admin1(
                ukr_admin1_shape, ukr_admin1_names[i], country, indicator
            )
        )

    admin1_indicator_gdf = pd.concat(admin1_indicator_gdf_list)

    export_directory = os.path.join(os.path.dirname(__file__), "temp")
    output_paths = make_detail_and_summary_file_paths(
        country, indicator, export_directory=export_directory
    )
    admin1_indicator_gdf.to_csv(output_paths["output_detail_path"], index=False)

    assert admin1_indicator_gdf.iloc[0].to_dict() == {
        "country_name": "Ukraine",
        "admin1_name": "Autonomous Republic of Crimea",
        "latitude": 46.17969,
        "longitude": 33.66016,
        "aggregation": "none",
        "indicator": "storm-europe",
        "value": 31.0,
    }

    assert len(admin1_indicator_gdf) == 43782


def test_get_date_range_from_live_api():
    date_range = get_date_range_from_live_api("crop-production")
    print(f"crop-production: {date_range}", flush=True)
    assert len(date_range) == 44
