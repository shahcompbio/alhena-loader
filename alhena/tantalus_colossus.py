
import os
import sys
import json
import logging
import collections
import math
import scipy.stats
import pandas as pd
import numpy as np
import alhena.constants as constants


def get_colossus_tantalus_data(directory):
    hmmcopy_data = collections.defaultdict(list)

    for table_name, data in load_qc_data(directory).items():
        hmmcopy_data[table_name].append(data)

    return hmmcopy_data

def get_colossus_tantalus_analysis_object(directory, dashboard_id, merged=None):
     if merged:
        metadata_filename = directory
    else:
        metadata_filename = os.path.join(
            directory, constants.METADATA_FILENAME)

    with open(metadata_filename) as metadata_file:
        metadata = json.load(metadata_file)

    standard_keys = ["sample_id", "description"]

    standard_keys.append(
        "libraries") if merged else standard_keys.append("library_id")

    for key in standard_keys:
        assert key in metadata.keys(), f"Missing {key} in metadata.json"

    record = {
        **metadata
    }

    record["jira_id"] = dashboard_id
    record["dashboard_id"] = dashboard_id
    record["dashboard_type"] = "merged" if merged else "single"

    return record

