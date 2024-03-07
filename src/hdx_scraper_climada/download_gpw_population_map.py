#!/usr/bin/env python

# The `.from_shape_and_countries` method for `litpop` (at least) requires the following file to
# be downloaded:
# http://sedac.ciesin.columbia.edu/downloads/data/gpw-v4/gpw-v4-population-count-rev11/gpw-v4-population-count-rev11_2020_30_sec_tif.zip
# and unzipped to
# (~\climada\data\gpw-v4-population-count-rev11_2020_30_sec_tif\ /
# gpw_v4_population_count_rev11_2020_30_sec.tif)
# This requires an account to be created for the download. An example script is provided to
# automate this download:
# https://urs.earthdata.nasa.gov/documentation/for_users/data_access/python

import datetime
import logging
import os
import zipfile
import requests

from pathlib import Path

from hdx.utilities.easy_logging import setup_logging

from hdx_scraper_climada.utilities import print_banner_to_log

setup_logging()
LOGGER = logging.getLogger(__name__)


# overriding requests.Session.rebuild_auth to mantain headers when redirected
class SessionWithHeaderRedirection(requests.Session):
    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (
                original_parsed.hostname != redirect_parsed.hostname
                and redirect_parsed.hostname != self.AUTH_HOST
                and original_parsed.hostname != self.AUTH_HOST
            ):
                del headers["Authorization"]


# create session with the user credentials that will be used to authenticate access to the data
def download_gpw_population(target_directory: str = None):
    print_banner_to_log(LOGGER, "Download GWP Population data")
    if target_directory is None:
        target_directory = os.path.join(
            os.path.expanduser("~"),
            "climada",
            "data",
            "gpw-v4-population-count-rev11_2020_30_sec_tif",
        )

    username = os.environ["NASA_EARTHDATA_USERNAME"]
    password = os.environ["NASA_EARTHDATA_PASSWORD"]
    session = SessionWithHeaderRedirection(username, password)
    # the url of the file we wish to retrieve
    url = (
        "http://sedac.ciesin.columbia.edu/downloads/data/gpw-v4/gpw-v4-population-count-rev11/"
        "gpw-v4-population-count-rev11_2020_30_sec_tif.zip"
    )
    # extract the filename from the url to be used when saving the file
    filename = url[(url.rfind("/") + 1) :]  # noqa: E203
    output_path = os.path.join(target_directory, filename)

    Path(target_directory).mkdir(parents=True, exist_ok=True)

    if os.path.exists(output_path):
        LOGGER.info(f"{output_path} is already present locally, returning with no action")
        return

    LOGGER.info(f"Downloading {url}")
    LOGGER.info(f"...to {output_path}")
    try:
        # submit the request using the session
        response = session.get(url, stream=True)
        print(response.status_code)
        # raise an exception in case of http errors
        response.raise_for_status()
        # save the file
        with open(output_path, "wb") as fd:
            for i, chunk in enumerate(response.iter_content(chunk_size=1024 * 1024)):
                fd.write(chunk)
                print(".", end="", flush=True)
        print("", flush=True)
    except requests.exceptions.HTTPError as e:
        # handle any errors here
        print(e)

    LOGGER.info(f"Download finished at {datetime.datetime.now().isoformat()}")
    with zipfile.ZipFile(output_path, "r") as zip_ref:
        LOGGER.info("Extracting files:")
        for file_ in zip_ref.filelist:
            LOGGER.info(f"\t{file_.filename}")
        zip_ref.extractall(os.path.dirname(output_path))
    LOGGER.info(f"Unzipping finished at {datetime.datetime.now().isoformat()}")


if __name__ == "__main__":
    download_gpw_population()
