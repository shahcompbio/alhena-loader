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
import scgenome.loaders.align
import scgenome.loaders.hmmcopy
import scgenome.loaders.annotation
from scgenome.db.qc_from_files import get_qc_data_from_filenames
import isabl_cli as ii
from alhena.tantalus_colossus import get_colossus_tantalus_data, get_colossus_tantalus_analysis_object


logger = logging.getLogger('alhena_loading')

chr_prefixed = {str(a): '0' + str(a) for a in range(1, 10)}





def load_analysis_from_dirs(dashboard_id, projects, host, port, alignment_dir, hmmcopy_dir, annotation_dir):
    
    qc_data = scgenome.loaders.align.load_align_data(alignment_dir)

    for table_name, data in scgenome.loaders.hmmcopy.load_hmmcopy_data(hmmcopy_dir).items():
        qc_data[table_name] = data

    for table_name, data in scgenome.loaders.annotation.load_annotation_data(annotation_dir).items():
        qc_data[table_name] = data

    load_data(dashboard_id, host, port, qc_data)
    
    analysis_record = get_isabl_analysis_object(dashboard_id)
    load_dashboard_entry(analysis_record,dashboard_id, host, port )
    add_dashboard_to_projects(dashboard_id, projects, host, port)

    logger.info("Done")

def load_analysis(dashboard_id,data,analysis_record, projects, directory, host, port):
    logger.info("====================== " + dashboard_id)
    load_data(dashboard_id, host, port,data)
    load_dashboard_entry(analysis_record,dashboard_id, host, port )
    add_dashboard_to_projects(dashboard_id, projects, host, port)
    logger.info("Done")





def load_merged_analysis(dashboard_id, projects, directory, host, port):

    metadata_dir = os.path.join(
        directory, constants.MERGED_DIRECTORYNAME, f'{dashboard_id}.json')

    assert os.path.exists(
        metadata_dir), f'Metadata file for {dashboard_id} does not exist in {os.path.join(directory, constants.MERGED_DIRECTORYNAME)}'

    with open(metadata_dir) as metadata_file:
        metadata = json.load(metadata_file)
        libraries = metadata["libraries"]

    add_columns = get_fitness_columns(
        directory) if "Fitness" in projects else []

    for library in libraries:
        library_directory = os.path.join(directory, library)
        hmmcopy_data = get_colossus_tantalus_data(library_directory)
        load_data(dashboard_id,
                  host, port, hmmcopy_data, add_columns=add_columns)

    analysis_record = get_colossus_tantalus_analysis_object(metadata_dir, dashboard_id,merged= True)

    load_dashboard_entry(analysis_record, dashboard_id,
                     host, port)

    add_dashboard_to_projects(dashboard_id, projects, host, port)



def load_data( dashboard_id, host, port, data, add_columns=[]):
    logger.info("LOADING DATA: " + dashboard_id)

    hmmcopy_data = data

    logger.info(f'loading hmmcopy data with tables {hmmcopy_data.keys()}')
    print(hmmcopy_data)
    for index_type in constants.DATA_TYPES:
        index_name = f"{dashboard_id.lower()}_{index_type}"
        logger.info(f"Index {index_name}")

        data = eval(f"get_{index_type}_data(hmmcopy_data)")
        
        #fitness case handler, was commented out for MSK igo, bccrc load
        
        if index_type == "qc" and len(add_columns) > 0:

            old_cell_count = data.shape[0]
            data = process_qc_fitness_data(data, add_columns)
           
            #this assertion will be wrong for a while :(, commented out
            #assert data.shape[0] == old_cell_count, "Missing cells after merge with new columns"
    
        load_records(data, index_name, host, port)

def process_qc_fitness_data(data,add_columns):

    column_df = pd.DataFrame(add_columns)
    #create temp_cell_id, merge on it, delete it
    column_df["temp_cell_id"] = column_df["cell_id"].astype(str)
    column_df["temp_cell_id"] = column_df["cell_id"].str.split(
        "-", 1).str[1]
    data["temp_cell_id"] = data["cell_id"].astype(str)
    data["temp_cell_id"] = data["cell_id"].str.split(
        "-", 1).str[1]
    #make room for new ordering
    data.drop('order', axis=1, inplace=True)
    #merge
    data = pd.merge(data, column_df, how="inner", on="temp_cell_id")
    del data["cell_id_x"]
    del data["temp_cell_id"]
    data = data.rename(columns={'cell_id_y': "cell_id"})

    return data

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



def load_dashboard_entry(analysis_object, dashboard_id, host, port):
    record = analysis_object
    # duplicate checking
    load_dashboard_record(record, dashboard_id, host, port)


'''
pass in an object which tells us what columns need to be relabeled
what it needs to be joined on

'''

def get_fitness_columns(directory):
    clone_df = pd.read_csv(os.path.join(
        directory, constants.MERGED_DIRECTORYNAME, "fitness_cell_assignment.csv"))
    clone_df = clone_df.rename(
        columns={"single_cell_id": "cell_id", "letters": "clone_id"})
    clone_df = clone_df[["cell_id", "clone_id"]]

    order_df = pd.read_csv(os.path.join(
        directory, constants.MERGED_DIRECTORYNAME, "cell_order.csv"))
    order_df = order_df.rename(
        columns={"label": "cell_id", "index": "order"})
    order_df = order_df[["cell_id", "order"]]

    return clone_df.merge(order_df).to_dict('records')

def get_custom_analysis_object(dashboard_id):
    
    record = {
        "sample_id": "",
        "jira_id":dashboard_id,
        "library_id":"",
        "description":""

    }
    return record