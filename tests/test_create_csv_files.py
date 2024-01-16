#!/usr/bin/env python
# encoding: utf-8

from hdx_scraper_climada.create_csv_files import export_indicator_data_to_csv


def test_export_indicator_data_to_csv():
    geodataframe_list = export_indicator_data_to_csv(country="Haiti")

    assert len(geodataframe_list) == 1
