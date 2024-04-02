# HDX-SCRAPER-CLIMADA

## Introduction

This code is designed to take data from the Climada API for the 23 Humanitarian Response Plan countries on HDX, aggregate it over subnational regions (admin1) where appropriate, export it to CSV and then publish it to HDX.

The data are all available under the ETH Zürich - Weather and Climate Risks organization on HDX:
https://data.humdata.org/organization/eth-zurich-weather-and-climate-risks

The source data in the CLIMADA API can be explored using this browser:
https://climada.ethz.ch/datasets/

The datasets published are lit population, crop_production, earthquake, flood, wildfire, tropical_cyclone and storm_europe.

## Disclaimer

These datasets were generated from the CLIMADA API which comes with the following disclaimer:

In this API we provide datasets in a form that can readily be used in CLIMADA analyses.

Users should determine whether these datasets are suitable for a particular purpose or application,
considering factors such as resolution (for example, a 4km grid is not suitable for modelling risk
at the neighborhood level), the way that hazards are represented in the dataset
(for example, specific events, event thresholds, probabilistic event sets, etc.),
the way that exposure is represented, and other aspects.

Data provided with no warranty of any kind under CC BY 4.0.

See respective API metadata and referenced publications for details and limitations.

Original at: https://climada.ethz.ch/disclaimer/


## Data format

Each dataset will have a a "country" CSV format file for each country where data are available and a CSV format summary file. The country file contains gridded data of the indicator, typically on a 4km grid. The summary file contains data summarised over the "admin1" level - this corresponds to a province or state. Both types of file include [HXL tags](https://hxlstandard.org/) on the second row of the dataset. Both files have the same columns:
```
country_name,admin1_name,latitude,longitude,aggregation,indicator,value
#country,#adm1+name,#geo+lat,#geo+lon,,#indicator+name,#indicator+num
```

The exposures datasets (earthquake, flood, wildfire, tropical cyclone and storm europe) also have a CSV format timeseries summary file. Which provides a summary value for each "event" in a country at admin1 level or better. These files have the following columns and HXL tags:

```
country_name,admin1_name,admin2_name,latitude,longitude,aggregation,indicator,event_date,value
#country,#adm1+name,#adm2+name,#geo+lat,#geo+lon,,#indicator+name,#date,#indicator+num
```

Where possible timeseries data is provided at the admin2 aggregation level

The country files have aggregation `none` and the summary files will have aggregation of either `sum` or `max`. The `indicator` column may be a compound value such as `crop_production.whe.noirr.USD` where an indicator calculation takes multiple values or it may be simple, such as `litpop`.

The summary file has a row per country per admin1 region per indicator whilst the country file has a row per underlying latitude / longitude grid point per indicator. 

Where the `admin1_name` are as per the private UN dataset [unmap-international-boundaries-geojson]([unmap-international-boundaries-geojson](https://data.humdata.org/dataset/unmap-international-boundaries-geojson)).

## Publication

The data are updated using GitHub Actions on this repository which run monthly on consecutive days at the beginning of each month. The datasets are only updated on HDX if the date range found in the data from API changes - we anticipate that this will happen yearly.

The flood indicator cannot be processed in GitHub Actions because it exceeds memory/time constrains. A monthly job will be run to highlight the need to consider a manual update which will require the code in this repository to be installed locally, as described below.

## Installation 
Create a virtual environment (assuming Windows for the `activate` command):

```shell
python -m venv venv
source venv/Scripts/activate
```

The Climada Python Library requires the GDAL library whose installation can be challenging. On the
Windows machine used for development the GDAL library is installed from a precompiled binary found
here: https://www.lfd.uci.edu/~gohlke/pythonlibs/

It is then installed with `pip`:

```shell
pip install GDAL-3.4.3-cp310-cp310-win_amd64.whl
```

This repository can then be cloned and installed with

```shell 
pip install -e .
```

The `.from_shape_and_countries` method for `litpop` (at least) requires the following file to be downloaded:

http://sedac.ciesin.columbia.edu/downloads/data/gpw-v4/gpw-v4-population-count-rev11/gpw-v4-population-count-rev11_2020_30_sec_tif.zip

to

~\climada\data\gpw-v4-population-count-rev11_2020_30_sec_tif\gpw_v4_population_count_rev11_2020_30_sec.tif

This can be done using the `hdx-climada` commandline tool, described below. This requires an account to be created on [https://urs.earthdata.nasa.gov/users/new](https://urs.earthdata.nasa.gov/users/new) for the download and the username and password stored in the environment variables `NASA_EARTHDATA_USERNAME` and `NASA_EARTHDATA_PASSWORD` respectively. The command to download the data is then:

```shell
hdx-climada download --data_name="population"
```

In addition UNMAP boundaries need to be downloaded from HDX. These are private datasets, not publically available. An appropriate HDX_KEY needs to be provided in an environment variable `HDX_KEY` for this download, and the upload of the completed datasets. The data can be downloaded using the `hdx-climada` commandline tool, described below. They can only be downloaded programmatically from the `prod` HDX site but can be downloaded manually from `stage` or elsewhere.

```shell
hdx-climada download --data_name="boundaries" --hdx_site="prod"
```

We use `nbstripout` to remove output cells from Jupyter Notebooks prior, this needs to be installed per repository with:

```
nbstripout --install
```

Note that this is not compatible with [GitKraken](https://www.gitkraken.com/).

The `write_image` function in `plotly` which is used to export figures to png format in Jupyter Notebook functions requires the `kaleido` library which is rather difficult to install on Windows 10. Simply installing with `pip` leads to a hang.

The solution is to downgrade to kaleido 0.1.0 and patch the library (file: Kaleido\scope\base.py - Line:70)! In the original `kaleido.cmd` reads `kaleido`.

```python
  @classmethod
  def executable_path(cls):
      vendored_executable_path = os.path.join(
          os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
          'executable',
          'kaleido.cmd'
  
      )
```

Described here:
https://github.com/plotly/Kaleido/issues/110#issuecomment-1021672450 

The dataset metadata are compiled in the file from the [CLIMADA data-type endpoint](https://climada.ethz.ch/data-types/):
```
\src\hdx_scraper_climada\metadata\2024-01-11-data-type-metadata.csv
```
But they are picked up by `create_datasets` from 
```
\src\hdx_scraper_climada\metadata\attributes.csv
``` 

## Commandline Interface

This repository implements a commandline interface using the `click` library, this is mainly concerned with the creation of the CLIMADA datasets for HDX but can also be used to download the datasets from HDX. It is accessed via the command `hdx-climada`. Help is provided by invoking `hdx-climada --help`:

```
Usage: hdx-climada [OPTIONS] COMMAND [ARGS]...

  Tools for the CLIMADA datasets in HDX

Options:
  --help  Show this message and exit.

Commands:
  create_dataset  Create a dataset in HDX with CSV files
  dataset_date    Show dataset date ranges
  download        Download data assets required to build the datasets
  info            Show the data_type info from the CLIMADA interface
```


## Dataset build details

A dataset can be generated manually with a commandline like:

```shell
hdx-climada create_dataset --indicator=$CLIMADA_INDICATOR --hdx_site=$HDX_SITE --live
```

The bulk properties of the datasets, built in GitHub Actions with the exception of flood:

|Indicator	|Size/MB|	Runtime	|Date range|
|-----------|-------|---------|----------|
|Lit population	|58	|34 minutes	|[2020-01-01T00:00:00 TO 2020-12-31T23:59:59]
|Crop production	|3.62	|6 minutes	|[2018-01-01T00:00:00 TO 2018-12-31T23:59:59]
|Earthquake	|58.5|	2 hours 11 minutes	|[1905-02-17T00:00:00 TO 2017-12-03T00:00:00]
|Flood	|239|	12 hours|	[2000-04-05T00:00:00 TO 2018-07-15T00:00:00]
|Wildfire	|44.9|	31 minutes	|[2001-01-01T00:00:00 TO 2020-01-01T00:00:00]
|Tropical-cyclone	|28.6|	58 minutes|	[1980-08-01T00:00:00 TO 2020-12-25T00:00:00]
|Storm-Europe|	11.5|	2 hours 22 minutes	|[1940-11-01T00:00:00 TO 2013-12-05T00:00:00]


### Crop production

Runtime for crop-production is about 134 seconds and generates 3.54MB of CSV files. This is smaller than for Litpop because although it comprises 8 datasets they are intrinsically lower resolution and do not form a complete grid.

### Earthquake

Runtime for Earthquake is about 10 minutes and generates 57MB of CSV files. Adding the time series summary increasing the time to generate data to about 3 hours.

The underlying data is historic records of earthquakes between 1905 and 2017. There are a little over 41,000 records. For each earthquake there is a map of the world with the intensity of shaking produced by the earthquake above a threshold of 4.0 units on the MMI - this will only have non-zero values over a relatively small area. The CLIMADA API provides a special function for showing the maximum intensity over all the earthquakes which is the data we present. This means there is a single value for each value on the map grid and we summarise over admin1 areas by taking the maximum intensity over the grid points in that area. 

Also included is a time series summary which shows the maximum intensity for each earthquake in each
admin1 area or admin2 if it is available.

### Flood

Runtime for Flood is about 12 hours and generates 239MB of data.

The underlying data is a binary mask (values either 0 or 1.) on a 200mx200m grid for each flood event. For the detail view this is sparse grid is stripped of non-zero values reducing the grid size from approximately 1 million points to O(10000). For the summary views the number of non-zero grid points is summed to provide an aggregate value per admin1 or admin2 area (where available).

Admin2 geometries are only available for Ethiopia, Haiti and Somalia

### Wildfire

Runtime for wildfire is about 30 minutes for 45MB

The underlying data is a fire intensity measured in Kelvin on a 4km grid, we retain this data for the country detail files but for both summary files we calculate a "fire extent" which is a count of the grid points for which there is a non-zero intensity.

### Tropical cyclone



### Storm Europe

Runtime for storm-europe is about 3 hours, generating 11MB. It is only run for Ukraine

The event date are supplied as a float which needs to be coerced to an int to convert to a date. 
Multiple events are recorded on each date, possibly representing hourly figures.

## Analysis

The Jupyter Notebook `data_explorer_notebook.ipynb` is used to check datasets "manually". The Excel spreadsheet `2024-01-27-pivot-table-haiti-admin1-crop_production.xlsx` demonstrates the use of PivotTables to convert data from a "narrow" format where there is a single indicator column potentially containing multiple indicator values for different attribute selections in the same set (i.e. `crop_production.whe.noirr.USD` and `crop_production.soy.noirr.USD`)





