import urllib3
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
import alhena.constants as constants
import os

import logging
logger = logging.getLogger('alhena_loading')

urllib3.disable_warnings()


DEFAULT_MAPPING = {
    "settings": {
        "index": {
            "max_result_window": 100000
        }
    },
    'mappings': {
        "dynamic_templates": [
            {
                "string_values": {
                    "match": "*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword"
                    }
                }
            }
        ]
    }
}


def initialize_es(host, port):
    assert os.environ['ALHENA_ES_USER'] is not None and os.environ['ALHENA_ES_PASSWORD'] is not None, 'Elasticsearch credentials missing'

    es = Elasticsearch(hosts=[{'host': host, 'port': port}],
                       http_auth=(os.environ['ALHENA_ES_USER'],
                                  os.environ['ALHENA_ES_PASSWORD']),
                       scheme='https',
                       timeout=300,
                       verify_certs=False)

    return es


def initialize_indices(host, port):
    es = initialize_es(host, port)

    logger.info('INITIALIZING ELASTICSEARCH')

    logger.info('Creating analyses')
    es.indices.create(index=constants.DASHBOARD_ENTRY_INDEX,
                      body=DEFAULT_MAPPING)

    logger.info('Creating default DLP dashboard')
    es.security.put_role(name="DLP_dashboardReader", body={
        'indices': [{
            'names': [constants.DASHBOARD_ENTRY_INDEX],
            'privileges': ["read"]
        }]
    }
    )


####################

def load_dashboard_record(record, dashboard_id, host, port):
    logger.info("Creating analysis object")
    load_record(record, dashboard_id,
                constants.DASHBOARD_ENTRY_INDEX, host, port)


def load_records(records, index_name, host, port, mapping=DEFAULT_MAPPING):
    es = initialize_es(host, port)

    if not es.indices.exists(index_name):
        logger.info(f'No index found - creating index named {index_name}')
        es.indices.create(
            index=index_name,
            body=mapping
        )

    for success, info in helpers.parallel_bulk(es, records, index=index_name):
        if not success:
            #   logging.error(info)
            logger.info(info)
            logger.info('Doc failed in parallel loading')


def load_record(record, record_id, index, host, port, mapping=DEFAULT_MAPPING):
    es = initialize_es(host, port)
    if not es.indices.exists(index):
        logger.info(f'No index found - creating index named {index}')
        es.indices.create(index=index, body=mapping)

    logger.info(f'Loading record')
    es.index(index=index, id=record_id, body=record)


###########


def clean_analysis(dashboard_id, host, port, projects=[]):
    logger.info("====================== " + dashboard_id)
    logger.info("Cleaning records")

    for data_type in constants.DATA_TYPES:
        logger.info(f"Deleting {data_type} records")
        delete_index(f"{dashboard_id.lower()}_{data_type}",
                     host=host, port=port)

    logger.info("DELETE DASHBOARD_ENTRY")
    delete_records(constants.DASHBOARD_ENTRY_INDEX,
                   dashboard_id, host=host, port=port)

    logger.info("Removing from projects")
    remove_dashboard_from_projects(dashboard_id, host, port, projects)


def delete_index(index, host="localhost", port=9200):
    es = initialize_es(host, port)
    if es.indices.exists(index):
        es.indices.delete(index=index, ignore=[400, 404])


def delete_records(index, filter_value, host="localhost", port=9200):
    es = initialize_es(host, port)

    if es.indices.exists(index):
        query = fill_base_query(filter_value)
        es.delete_by_query(index=index, body=query, refresh=True)


#####

def is_loaded(dashboard_id, host, port):
    es = initialize_es(host, port)

    query = fill_base_query(dashboard_id)
    count = es.count(body=query, index=constants.DASHBOARD_ENTRY_INDEX)

    return count["count"] == 1


def fill_base_query(value):
    return {
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        "dashboard_id": value
                    }
                }
            }
        }
    }


# PROJECTS


def get_projects(host, port):
    es = initialize_es(host, port)

    response = es.security.get_role()
    projects = [response_key[:-len("_dashboardReader")] for response_key in response.keys(
    ) if response_key.endswith("_dashboardReader")]

    return projects


def is_project_exist(project, host, port):
    es = initialize_es(host, port)

    dashboard_name = f'{project}_dashboardReader'

    try:
        result = es.security.get_role(name=dashboard_name)
        return dashboard_name in result

    except NotFoundError:
        return False


def add_project(project_name, dashboards, host, port):
    es = initialize_es(host, port)

    project_role_name = f'{project_name}_dashboardReader'

    es.security.put_role(name=project_role_name, body={
        'indices': [{
            'names': [constants.DASHBOARD_ENTRY_INDEX] + dashboards,
            'privileges': ["read"]
        }]
    }
    )
    logger.info(f'Added new project: {project_name} ')


def add_dashboard_to_projects(dashboard_id, projects, host, port):

    es = initialize_es(host, port)

    for project in projects:

        logger.info(f'Adding {dashboard_id} to {project} list')
        project_role_name = f'{project}_dashboardReader'

        project_role = es.security.get_role(name=project_role_name)
        project_indices = list(
            project_role[project_role_name]["indices"][0]["names"])

        if dashboard_id in project_indices:
            continue
        else:
            project_indices.append(dashboard_id)
            es.security.put_role(name=project_role_name, body={
                'indices': [{
                    'names': project_indices,
                    'privileges': ["read"]
                }]
            }
            )


def remove_dashboard_from_projects(dashboard_id, host, port, projects):
    es = initialize_es(host, port)

    if len(projects) > 0:
        projects = [
            f"{proj}_dashboardReader" for proj in projects]

        logger.info(f'Removing {dashboard_id} from {len(projects)} projects')

        for project in projects:
            response = es.security.get_role(name=project)

            project_data = response[project]
            project_indices = list(project_data["indices"][0]["names"])

            if dashboard_id in project_indices:
                logger.info(f'Removing {dashboard_id} from {project}')

                project_indices.remove(dashboard_id)

                es.security.put_role(name=project, body={
                    'indices': [{
                        'names': project_indices,
                        'privileges': ["read"]
                    }]
                }
                )
    else:
        response = es.security.get_role()
        projects = [response_key for response_key in response.keys(
        ) if response_key.endswith("_dashboardReader")]

        logger.info(f'Removing {dashboard_id} from {len(projects)} projects')

        for project in projects:
            project_data = response[project]
            project_indices = list(project_data["indices"][0]["names"])

            if dashboard_id in project_indices:
                logger.info(f'Removing {dashboard_id} from {project}')

                project_indices.remove(dashboard_id)

                es.security.put_role(name=project, body={
                    'indices': [{
                        'names': project_indices,
                        'privileges': ["read"]
                    }]
                }
                )
