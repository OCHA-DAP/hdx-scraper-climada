# HDX-SCRAPER-CLIMADA

## Introduction

This code is designed to take data from the Climada API for the 23 Humanitarian Response Plan countries on HDX, aggregate it over subnational regions (admin1) where appropriate, export it to CSV and then publish it to HDX.

Getting started:
https://github.com/CLIMADA-project/climada_python/blob/main/doc/tutorial/climada_util_api_client.ipynb

The data can be explored using this browser:
https://climada.ethz.ch/datasets/

The maintainer for this dataset is set in the `climada-litpop.json` file to `emanuel-schmid-3262` and the organisation to `eth-zurich-weather-and-climate-risks`

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


To use the UNMAP boundaries the LitPop.from_shape_and_countries needs to be monkey patched. In a virtual environment this file needs to be
changed at line 424 replacing `gdf = gdf.append(...` with `gdf = gdf._append(...`

```
venv\Lib\site-packages\climada\entity\exposures\litpop\litpop.py
```

We use `nbstripout` to remove output cells from Jupyter Notebooks prior, this needs to be installed per repository with:

```
nbstripout --install
```

## Data format

Each dataset will have a a "country" CSV format file for each country where data are available and a CSV format summary file. Both types of file include HXL tags on the second row of the dataset. Both files have the same columns:
```
country_name,region_name,latitude,longitude,aggregation,indicator,value
#country,#adm1+name,#geo+lat,#geo+lon,,#indicator+name,#indicator+num
```

The country files have aggregation `none` and the summary files will have aggregation `sum`. The `indicator` column may be a compound value such as `crop_production.whe.noirr.USD` where an indicator calculation takes multiple values or it may be simple, such as `litpop`.

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

## Dataset updates

The data are generated using `make run`. Currently updates are manual since there are unresolved challenges in running the process in a GitHub Action.

### LitPop

Runtime for Litpop is about 50 minutes and generates 58MB of CSV files.

### Crop production

Runtime for crop-production is about 134 seconds and generates 3.54MB of CSV files. This is smaller than for Litpop because although it comprises 8 datasets they are intrinsically lower resolution and do not form a complete grid.

### Relative cropyield

## Analysis


The Jupyter Notebook `data_explorer_notebook.ipynb` is used to check datasets "manually". The Excel spreadsheet `2024-01-27-pivot-table-haiti-admin1-crop_production.xlsx` demonstrates the use of PivotTables to convert data from a "narrow" format where there is a single indicator column potentially containing multiple indicator values for different attribute selections in the same set (i.e. `crop_production.whe.noirr.USD` and `crop_production.soy.noirr.USD`)





