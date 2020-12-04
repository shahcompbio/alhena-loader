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
from alhena.elasticsearch import initialize_es, load_dashboard_record, load_records as _load_records, add_dashboard_to_projects
from scgenome.loaders.qc import load_qc_data

logger = logging.getLogger('alhena_loading')

chr_prefixed = {str(a): '0' + str(a) for a in range(1, 10)}


def load_analysis(dashboard_id, projects, directory, host, port):
    logger.info("====================== " + dashboard_id)
    load_data(directory, dashboard_id, host, port)
    load_dashboard_entry(directory, dashboard_id, host, port)
    add_dashboard_to_projects(dashboard_id, projects, host, port)
    logger.info("Done")


# !!! TODO
# This function assumes that the directory structure is:
# directory
# - library #1 (folder)
# - all data for library #1
# - library #2 (folder)
# - all data for library #2

# This function will take the list of library IDs, and load all the each library into
# indices named after dashboard_id:
# <dashboard_id>_qc
# <dashboard_id>_segs
# <dashboard_id>_bins
# <dashboard_id>_qc

# IDEALLY We should try and utilize load_data for the actual transformation / loading
# So this function may just be built to feed load_data the relevant arguments
def load_merged_analysis(dashboard_id, libraries, projects, directory, host, port):
    return


def load_data(directory, dashboard_id, host, port):
    logger.info("LOADING DATA: " + dashboard_id)

    hmmcopy_data = collections.defaultdict(list)

    for table_name, data in load_qc_data(directory).items():
        hmmcopy_data[table_name].append(data)
    for table_name in hmmcopy_data:
        hmmcopy_data[table_name] = pd.concat(
            hmmcopy_data[table_name], ignore_index=True)

    logger.info(f'loading hmmcopy data with tables {hmmcopy_data.keys()}')

    for index_type in constants.DATA_TYPES:
        index_name = f"{dashboard_id.lower()}_{index_type}"
        logger.info(f"Index {index_name}")

        data = eval(f"get_{index_type}_data(hmmcopy_data)")

        logger.info(f"dataframe for {index_name} has shape {data.shape}")
        load_records(data, index_name, host, port)


def get_qc_data(hmmcopy_data):
    data = hmmcopy_data['annotation_metrics']
    data['percent_unmapped_reads'] = data["unmapped_reads"] / data["total_reads"]
    data['is_contaminated'] = data['is_contaminated'].apply(
        lambda a: {True: 'true', False: 'false'}[a])
    return data


def get_segs_data(hmmcopy_data):
    data = hmmcopy_data['hmmcopy_segs'].copy()
    data['chrom_number'] = create_chrom_number(data['chr'])
    return data


def get_bins_data(hmmcopy_data):
    data = hmmcopy_data['hmmcopy_reads'].copy()
    data['chrom_number'] = create_chrom_number(data['chr'])
    return data


def get_gc_bias_data(hmmcopy_data):
    data = hmmcopy_data['gc_metrics']

    gc_cols = list(range(101))
    gc_bias_df = pd.DataFrame(columns=['cell_id', 'gc_percent', 'value'])
    for n in gc_cols:
        new_df = data.loc[:, ['cell_id', str(n)]]
        new_df.columns = ['cell_id', 'value']
        new_df['gc_percent'] = n
        gc_bias_df = gc_bias_df.append(new_df, ignore_index=True)

    return gc_bias_df


def create_chrom_number(chromosomes):
    chrom_number = chromosomes.map(lambda a: chr_prefixed.get(a, a))
    return chrom_number


GET_DATA = {
    f"qc": get_qc_data,
    f"segs": get_segs_data,
    f"bins": get_bins_data,
    f"gc_bias": get_gc_bias_data,
}


def load_records(data, index_name, host, port):

    total_records = data.shape[0]
    num_records = 0

    batch_size = int(1e5)
    for batch_start_idx in range(0, data.shape[0], batch_size):
        batch_end_idx = min(batch_start_idx + batch_size, data.shape[0])
        batch_data = data.loc[data.index[batch_start_idx:batch_end_idx]]

        clean_fields(batch_data)

        records = []
        for record in batch_data.to_dict(orient='records'):
            clean_nans(record)
            records.append(record)

        _load_records(records, index_name, host, port)
        num_records += batch_data.shape[0]
        logger.info(
            f"Loading {len(records)} records. Total: {num_records} / {total_records} ({round(num_records * 100 / total_records, 2)}%)")

    if total_records != num_records:
        raise ValueError(
            f'mismatch in {num_cells} cells loaded to {total_cells} total cells')


def clean_fields(data):
    invalid_chars = ['.']
    invalid_cols = [col for col in data.columns if any(
        [char in col for char in invalid_chars])]
    for col in invalid_cols:
        found_chars = [char for char in invalid_chars if char in col]

        for char in found_chars:
            new_col = col.replace(char, '_')
            data.rename(columns={col: new_col}, inplace=True)


def clean_nans(record):
    floats = [field for field in record if isinstance(record[field], float)]
    for field in floats:
        if np.isnan(record[field]):
            del record[field]


def load_dashboard_entry(directory, dashboard_id, host, port):
    logger.info("LOADING DASHBOARD ENTRY: " + dashboard_id)

    metadata_filename = os.path.join(directory, constants.METADATA_FILENAME)

    with open(metadata_filename) as metadata_file:
        metadata = json.load(metadata_file)

    for key in ["sample_id", "library_id", "description"]:
        assert key in metadata.keys(), f"Missing {key} in metadata.json"

    record = {
        "dashboard_id": dashboard_id,
        "sample_id": metadata["sample_id"],
        "library_id": metadata["library_id"],
        "jira_id": dashboard_id,
        "description": metadata["description"]
    }

    load_dashboard_record(record, dashboard_id, host, port)
