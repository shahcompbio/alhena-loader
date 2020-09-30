import os
import json
import logging
logger = logging.getLogger('alhena_loading')
from scgenome.db.qc import cache_qc_results
import alhena.constants as constants

def download_analysis(dashboard_id, data_directory, sample_id, library_id, description):
    directory = os.path.join(data_directory, dashboard_id)

    assert not os.path.exists(directory), f"Directory {directory} already exists"

    ## Download data from Tantalus
    logger.info("Downloading data")
    cache_qc_results(dashboard_id, directory)

    ## Create analysis metadata file
    create_analysis_metadata(dashboard_id, directory, sample_id, library_id, description)
    
    return directory


def create_analysis_metadata(dashboard_id, directory, sample_id, library_id, description):
    metadata = {
        "dashboard_id": dashboard_id,
        "sample_id": sample_id,
        "library_id": library_id,
        "description": description
    }

    logger.info("Creating metadata file")
    with open(os.path.join(directory, constants.METADATA_FILENAME), 'w+') as outfile:
        json.dump(metadata, outfile)