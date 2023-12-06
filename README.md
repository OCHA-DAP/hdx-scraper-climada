# HDX-SCRAPER-CLIMADA

## Overview

This code is designed to take data from the Climada API for the 23 Data Grid countries on HDX,
aggregate it over subnational regions (admin1) where appropriate, export it to CSV and then publish
it to HDX>

Getting started:
https://github.com/CLIMADA-project/climada_python/blob/main/doc/tutorial/climada_util_api_client.ipynb

## Development environment
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
following, assuming the host machine is Windows based. GDAL is installed using a precompiled binary
sourced from 

```shell
pip install climada
```






