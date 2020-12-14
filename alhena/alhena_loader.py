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
from alhena.alhena_data import download_analysis

logger = logging.getLogger('alhena_loading')

chr_prefixed = {str(a): '0' + str(a) for a in range(1, 10)}

def load_analysis(dashboard_id, projects, directory, host, port):
    logger.info("====================== " + dashboard_id)
    load_data(directory, dashboard_id, host, port)
    load_dashboard_entry(directory, dashboard_id, host, port)
    add_dashboard_to_projects(dashboard_id, projects, host, port)
    logger.info("Done")

# part_1
def load_merged_analysis(dashboard_id, libraries, projects, directory, host, port):

    # get location of metadata
    metadatalocation = get_metadata_location(directory, dashboard_id)
    # open and load metadata
    with open(metadatalocation) as metadata_file:
        metadata = json.load(metadata_file)
        libraries = metadata["libraries"]
    # create analysis object and pass metadatalocation to it
    load_dashboard_entry(metadatalocation, dashboard_id,
                         host, port, merged=True)
    part_2
    add_dashboard_to_projects(dashboard_id, projects, host, port)

    # load each library
    
    for library in libraries:
        libraryFolder = os.path.join(directory, library)
        #print(libraryFolder)
        load_data(libraryFolder, dashboard_id, host, port, heatmap_order=True)
    

# verify? check? download?
#part 5.1
def bccrc_verify_libraries(directory, dashboard_id):
    '''
    we check the metadata.json in dat/alhena/merged 
    read libraries
    check our dat/alhena/sc-test if it has these libraries loaded in
    just check if sub directory is named after that
    if it does not have libraries loaded in, we load them in via tantalus one liner :)
    continue load_merged_analysis
    '''
    metadatalocation = get_metadata_location(directory, dashboard_id)
    # open and load metadata
    with open(metadatalocation) as metadata_file:
        metadata = json.load(metadata_file)
        metadata_libraries = metadata["libraries"]
    '''
    print("libraries From metadata")
    for library in metadata_libraries:
        print(library)
    '''

    # get all dashboard_ids in current directory : dat/alhena
    data_folders = [f.name for f in os.scandir(directory) if f.is_dir()]
    # get dashboard_ids in meta_libraries that are NOT in dat/alhena aka missing folders in dat/alhena
    missing_data_ids = np.setdiff1d(metadata_libraries, data_folders)
    # numpy truth check aka if returned numpy array is empty
    if missing_data_ids.size != 0:
        logger.info(f"downloading missing folders to {directory} ")
        for missing_data_id in missing_data_ids:
            logger.info(f"loading {missing_data_id} into {directory}")
            download_analysis(missing_data_id, directory)
    else:
        logger.info(f"No Missing Data Folders in {directory} ")


def get_metadata_location(directory, dashboard_id):
    merged_folder = os.path.join(directory, constants.MERGED_DIRECTORYNAME)
    # these are the  SC-whatevers.json folders
    merged_subfolders = [f.name for f in os.scandir(
        merged_folder) if f.is_file()]

    metadataFile = f"{dashboard_id}.json"
    # lets hit the merged.json here...where do we go? we go to merged
    assert metadataFile in merged_subfolders, f"{dashboard_id}.json missing in /merged, add it to Merged Directory,returning"
    # get location of metadata
    metadata_location = os.path.join(merged_folder, metadataFile)
    return metadata_location



def load_data(directory, dashboard_id, host, port, heatmap_order=None):
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
        # This is where we match_heatmap_ordering
        #part_4
        if index_type == "qc" and heatmap_order:
            data = match_heatmap_order(data)
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


def load_dashboard_entry(directory, dashboard_id, host, port, merged=None):
    logger.info("LOADING DASHBOARD ENTRY: " + dashboard_id)
    if merged:
        metadata_filename = directory
    else:
        metadata_filename = os.path.join(
            directory, constants.METADATA_FILENAME)

    with open(metadata_filename) as metadata_file:
        metadata = json.load(metadata_file)

    standard_keys = ["sample_id", "description"]
    # metadata of merged should have libraries field not library_id
    # metadata of single has library_id
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
    # duplicate checking
    load_dashboard_record(record, dashboard_id, host, port)

#part_4
def match_heatmap_order(data):
    # needs location of heatmap_referencedata
    with open('alhena/testdata/final_heatmap.csv') as refdatafile:
        refdata_df = pd.read_csv(refdatafile, index_col=[0])

        matched_data = pd.merge(data, refdata_df,
                                on="cell_id", suffixes=("_prev", ""))
        matched_data.drop("order_prev", axis=1, inplace=True)
        matched_data.to_csv('alhena/testdata/output.csv', mode="a")
        return matched_data


