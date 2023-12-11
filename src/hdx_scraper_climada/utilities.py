#!/usr/bin/env python
# encoding: utf-8

import csv
import os

from typing import Any


def write_dictionary(
    output_filepath: str, output_rows: list[dict[str, Any]], append: bool = True
) -> str:
    keys = list(output_rows[0].keys())
    newfile = not os.path.isfile(output_filepath)

    if not append and not newfile:
        os.remove(output_filepath)
        newfile = True

    with open(output_filepath, "a", encoding="utf-8", errors="ignore") as output_file:
        dict_writer = csv.DictWriter(
            output_file,
            keys,
            lineterminator="\n",
        )
        if newfile:
            dict_writer.writeheader()
        dict_writer.writerows(output_rows)

    status = _make_write_dictionary_status(append, output_filepath, newfile)

    return status


def _make_write_dictionary_status(append: bool, filepath: str, newfile: bool) -> str:
    status = ""
    if not append and not newfile:
        status = f"Append is False, and {filepath} exists therefore file is being deleted"
    elif not newfile and append:
        status = f"Append is True, and {filepath} exists therefore data is being appended"
    else:
        status = f"New file {filepath} is being created"
    return status
