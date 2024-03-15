# HDX-SCRAPER-CLIMADA

## Introduction

This code is designed to take data from the Climada API for the 23 Humanitarian Response Plan countries on HDX, aggregate it over subnational regions (admin1) where appropriate, export it to CSV and then publish it to HDX.

Getting started:
https://github.com/CLIMADA-project/climada_python/blob/main/doc/tutorial/climada_util_api_client.ipynb

The data can be explored using this browser:
https://climada.ethz.ch/datasets/

The maintainer for this dataset is set in the `climada-litpop.json` file to `emanuel-schmid-3262` and the organisation to `eth-zurich-weather-and-climate-risks`

## Publication

Since these datasets are processed locally the following process is followed to publish to production:
1. Set `INDICATOR` to correct value in `run.py`
2. Set `DRY_RUN` to `False` in `run.py`
3. Set the `HDX_SITE` to `prod` in `~/.hdx_configuration.yaml` or in `create_datasets.py`

This changes should be reverted after publication. The resources should have been generated prior to publication with publication itself taking 5-10 minutes

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

The climada Python Library (https://github.com/CLIMADA-project/climada_python) is installed with the
following, assuming the host machine is Windows based. 

```shell
pip install climada
```

The `.from_shape_and_countries` method for `litpop` (at least) requires the following file to be downloaded:

http://sedac.ciesin.columbia.edu/downloads/data/gpw-v4/gpw-v4-population-count-rev11/gpw-v4-population-count-rev11_2020_30_sec_tif.zip

to

~\climada\data\gpw-v4-population-count-rev11_2020_30_sec_tif\gpw_v4_population_count_rev11_2020_30_sec.tif

This requires an account to be created for the download. An example script is provided to automate this download:

https://urs.earthdata.nasa.gov/documentation/for_users/data_access/python


To use the UNMAP boundaries the LitPop.from_shape_and_countries we need to use a patched version of the LitPop class. This is done by copying the original `litpop.py` which implements the class into this repo and importing LitPop from this patched version with

```
from hdx_scraper_climada.patched_litpop import LitPop
```

The change made is at approximately line 424 replacing `gdf = gdf.append(...` with `gdf = gdf._append(...`

```
venv\Lib\site-packages\climada\entity\exposures\litpop\litpop.py
```

We use `nbstripout` to remove output cells from Jupyter Notebooks prior, this needs to be installed per repository with:

```
nbstripout --install
```

The `write_image` function in `plotly` which is used to export figures to png format in Jupyter Notebook functions requires the `kaleido` library which is rather difficult to install on Windows 10. Simply installing with `pip` leads to a hang when an attempt is

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

## Data format

Each dataset will have a a "country" CSV format file for each country where data are available and a CSV format summary file.  Both types of file include HXL tags on the second row of the dataset. Both files have the same columns:
```
country_name,region_name,latitude,longitude,aggregation,indicator,value
#country,#adm1+name,#geo+lat,#geo+lon,,#indicator+name,#indicator+num
```

Some datasets also have a CSV format timeseries summary file, these have the following columns and HXL tags:

```
country_name,admin1_name,admin2_name,latitude,longitude,aggregation,indicator,event_date,value
#country,#adm1+name,#adm2+name,#geo+lat,#geo+lon,,#indicator+name,#date,#indicator+num
```

Where possible timeseries data is provided at the admin2 aggregation level

The country files have aggregation `none` and the summary files will have aggregation of either `sum` or `max`. The `indicator` column may be a compound value such as `crop_production.whe.noirr.USD` where an indicator calculation takes multiple values or it may be simple, such as `litpop`.

The summary file has a row per country per admin1 region per indicator whilst the country file has a row per underlying latitude / longitude grid point per indicator. 

Where the `region_name` are `admin1` names as per the private UNOCHA dataset [unmap-international-boundaries-geojson]([unmap-international-boundaries-geojson](https://data.humdata.org/dataset/unmap-international-boundaries-geojson)).

The dataset metadata are compiled in the file from the [CLIMADA data-type endpoint](https://climada.ethz.ch/data-types/):
```
\src\hdx_scraper_climada\metadata\2024-01-11-data-type-metadata.csv
```
But they are picked up by `create_datasets` from 
```
\src\hdx_scraper_climada\metadata\attributes.csv
``` 

## Commandline Interface

This repository implements a commandline interface using the `click` library. It is asked via the command `hdx-climada`. Help is provided by invoking `hdx-climada --help`

## Dataset updates

The data are generated using `make run`. Currently updates are manual since there are unresolved challenges in running the process in a GitHub Action.

### LitPop

Runtime for Litpop is about 50 minutes and generates 58MB of CSV files.

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

### River flood

Runtime for river-flood is about 75 minutes for 46MB (single model)


### Storm Europe

Runtime for storm-europe is about 3 hours, generating 11MB. It is only run for Ukraine

The event date are supplied as a float which needs to be coerced to an int to convert to a date. 
Multiple events are recorded on each date, possibly representing hourly figures.

### Relative cropyield

## Analysis


The Jupyter Notebook `data_explorer_notebook.ipynb` is used to check datasets "manually". The Excel spreadsheet `2024-01-27-pivot-table-haiti-admin1-crop_production.xlsx` demonstrates the use of PivotTables to convert data from a "narrow" format where there is a single indicator column potentially containing multiple indicator values for different attribute selections in the same set (i.e. `crop_production.whe.noirr.USD` and `crop_production.soy.noirr.USD`)





