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

This requires an account to be created for the download.

To use the UNMAP boundaries the LitPop.from_shape_and_countries needs to be monkey patched. In a virtual environment this file needs to be
changed at line 424 replacing `gdf = gdf.append(...` with `gdf = gdf._append(...`

```
venv\Lib\site-packages\climada\entity\exposures\litpop\litpop.py
```

We use `nbstripout` to remove output cells from Jupyter Notebooks prior, this needs to be installed per repository with:

```
nbstripout --install
```

## Dataset updates

The data are generated using `make run`. Currently updates are manual since there are unresolved challenges in running the process in a GitHub Action

### LitPop

There is a nice explanation of the LitPop data at the beginning of the tutorial notebook:

```
[doc/tutorial/climada_entity_LitPop.ipynb](https://github.com/CLIMADA-project/climada_python/blob/main/doc/tutorial/climada_entity_LitPop.ipynb)
```

Runtime for Litpop is about 50 minutes and generates 58MB of CSV files.

### Crop production







