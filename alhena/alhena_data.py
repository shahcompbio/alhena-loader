import alhena.constants as constants
from scgenome.db.qc import cache_qc_results
import os
import json
import logging
import response
import requests
logger = logging.getLogger('alhena_loading')


__BASE_URL = "https://colossus.canadacentral.cloudapp.azure.com/api"


def download_libaries_for_merged(directory, dashboard_id):

    metadata_dir = os.path.join(
        directory, constants.MERGED_DIRECTORYNAME, f'{dashboard_id}.json')

    assert os.path.exists(
        metadata_dir), f'Metadata file for {dashboard_id} does not exist in {os.path.join(directory, constants.MERGED_DIRECTORYNAME)}'

    with open(metadata_dir) as metadata_file:
        metadata = json.load(metadata_file)
        metadata_libraries = metadata["libraries"]

    logger.info(f'Checking for {len(metadata_libraries)} libraries')
    missing_libraries = [
        library for library in metadata_libraries if not os.path.exists(os.path.join(directory, library))]

    if len(missing_libraries) != 0:
        logger.info(
            f"Downloading {len(missing_libraries)} libraries to {directory}")
        for library_id in missing_libraries:
            download_analysis(library_id, directory)
    else:
        logger.info(f"No Missing Data Folders in {directory} ")


def download_analysis(dashboard_id, data_directory):
    directory = os.path.join(data_directory, dashboard_id)

    # !!! TODO want to change this assert to just a pass - that is, if path exists already, then just return directory without downloading
    assert not os.path.exists(
        directory), f"Directory {directory} already exists"

    # Download data from Tantalus
    logger.info(f'Downloading {dashboard_id} to {directory} ')
    cache_qc_results(dashboard_id, directory)

    # Create analysis metadata file
    create_analysis_metadata(dashboard_id, directory)

    return directory


def create_analysis_metadata(dashboard_id, directory):
    user = os.environ['COLOSSUS_API_USERNAME']
    password = os.environ['COLOSSUS_API_PASSWORD']

    response = requests.get(
        constants.COLOSSUS_BASE_URL + '/analysis_information/?analysis_jira_ticket=' + dashboard_id, auth=(user, password))

    data = response.json()['results'][0]
    metadata = get_metadata_record(data, dashboard_id)

    logger.info("Creating metadata file")
    with open(os.path.join(directory, constants.METADATA_FILENAME), 'w+') as outfile:
        json.dump(metadata, outfile)


def get_metadata_record(analysis, dashboard_id):
    return {
        "sample_id": analysis["library"]["sample"]["sample_id"],
        "library_id": analysis["library"]["pool_id"],
        "dashboard_id": dashboard_id,
        "description": analysis["library"]["description"]
    }
