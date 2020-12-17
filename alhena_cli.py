import click
import logging
import logging.handlers
import os

from alhena.alhena_loader import load_analysis as _load_analysis, load_merged_analysis as _load_merged_analysis
from alhena.alhena_data import download_analysis as _download_analysis, download_libraries_for_merged as _download_libraries_for_merged
from alhena.elasticsearch import clean_analysis as _clean_analysis, is_loaded as _is_loaded, is_project_exist as _is_project_exist, initialize_indices as _initialize_es_indices, add_project as _add_project, get_projects as _get_projects
import alhena.constants as constants

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"


@click.group()
@click.option('--host', default='localhost', help='Hostname for Elasticsearch server')
@click.option('--port', default=9200, help='Port for Elasticsearch server')
@click.option('--debug', is_flag=True, help='Turn on debugging logs')
@click.pass_context
def main(ctx, host, port, debug):
    ctx.obj['host'] = host
    ctx.obj['port'] = port

    level = logging.DEBUG if debug else logging.INFO

    os.makedirs('logs/', exist_ok=True)
    handler = logging.handlers.TimedRotatingFileHandler(
        'logs/alhena-log.log', 'midnight', 1)
    handler.suffix = "%Y-%m-%d"

    logging.basicConfig(format=LOGGING_FORMAT, handlers=[
                        handler, logging.StreamHandler()])

    logger = logging.getLogger('alhena_loading')
    logger.setLevel(level)

    ctx.obj['logger'] = logger


@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--id', help="ID of dashboard", required=True)
@click.option('--project', 'projects', multiple=True, default=["DLP"], help="Projects to load dashboard into")
@click.option('--reload', is_flag=True, help="Force reload this dashboard")
def load_analysis(ctx, data_directory, id, projects, reload):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    assert reload or not _is_loaded(
        id, es_host, es_port), f'Dashboard with ID {id} already loaded. To reload, add --reload to command'

    nonexistant_projects = [project for project in projects if not _is_project_exist(
        project, es_host, es_port)]

    assert len(
        nonexistant_projects) == 0, f'Projects do not exist: {nonexistant_projects} '

    if reload:
        _clean_analysis(id, host=es_host, port=es_port)

    _load_analysis(id, projects, data_directory, es_host, es_port)


@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--id', help="ID of dashboard", required=True)
@click.option('--project', 'projects', multiple=True, default=["DLP"], help="Projects to load dashboard into")
@click.option('--reload', is_flag=True, help="Force reload this dashboard")
def load_merged_analysis(ctx, data_directory, id, projects, reload):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    assert reload or not _is_loaded(
        id, es_host, es_port), f'Dashboard with ID {id} already loaded. To reload, add --reload to command'

    nonexistant_projects = [project for project in projects if not _is_project_exist(
        project, es_host, es_port)]

    assert len(
        nonexistant_projects) == 0, f'Projects do not exist: {nonexistant_projects} '

    if reload:
        _clean_analysis(id, host=es_host, port=es_port)

    _load_merged_analysis(id, projects,
                          data_directory, es_host, es_port)


@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--id', help="ID of dashboard", required=True)
@click.option('--project', 'projects', multiple=True, default=["DLP"], help="Projects to load dashboard into")
@click.option('--reload', is_flag=True, help="Force reload this dashboard")
# part_5
def load_merged_analysis_bccrc(ctx, data_directory, id, projects, reload):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]
    # get metadata.json ,sc-test.json, id.json located in the data directory
    # check to see if those libraries exist
    # oneline new function called bccrc_verify_libraries()

    assert reload or not _is_loaded(
        id, es_host, es_port), f'Dashboard with ID {id} already loaded. To reload, add --reload to command'

    nonexistant_projects = [project for project in projects if not _is_project_exist(
        project, es_host, es_port)]

    assert len(
        nonexistant_projects) == 0, f'Projects do not exist: {nonexistant_projects} '

    if reload:
        _clean_analysis(id, host=es_host, port=es_port)

    _download_libraries_for_merged(id, data_directory)

    _load_merged_analysis(id, projects,
                          data_directory, es_host, es_port)


@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--id', help="ID of dashboard", required=True)
@click.option('--project', 'projects', multiple=True, default=["DLP"], help="Projects to load dashboard into")
@click.option('--download', is_flag=True, help="Download data")
@click.option('--reload', is_flag=True, help="Force reload this dashboard")
def load_analysis_shah(ctx, data_directory, id, projects, download, reload):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    assert reload or not _is_loaded(
        id, es_host, es_port), f'{id} already loaded. To reload, add --reload to command'

    nonexistant_projects = [project for project in projects if not _is_project_exist(
        project, es_host, es_port)]

    assert len(
        nonexistant_projects) == 0, f'Projects do not exist: {nonexistant_projects} '

    if download:
        data_directory = _download_analysis(
            id, data_directory)

    if reload:
        _clean_analysis(id, host=es_host, port=es_port)

    _load_analysis(id, projects, data_directory, es_host, es_port)


@main.command()
@click.option('--project', 'projects', multiple=True, help="List of project names")
@click.pass_context
def verify_projects(ctx, projects):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    logger = ctx.obj["logger"]

    good_projects = []
    bad_projects = []
    for project in projects:
        if _is_project_exist(project, es_host, es_port):
            good_projects.append(project)
        else:
            bad_projects.append(project)

    logger.info(f'==== Verified project names: {len(good_projects)}')
    for project in good_projects:
        logger.info(project)

    logger.info(
        f'==== Incorrect / Missing project names: {len(bad_projects)} ')
    for project in bad_projects:
        logger.info(project)


@main.command()
@click.pass_context
def all_projects(ctx):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]
    logger = ctx.obj["logger"]
    # want to show all projects in given ES
    projects = _get_projects(es_host, es_port)

    logger.info(f'==== All project names for {es_host}:{es_port}')
    for project in projects:
        logger.info(project)


@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--id', help="ID of dashboard", required=True)
@click.option('--project', 'projects', multiple=True, default=["DLP"], help="Projects to load dashboard into")
@click.option('--download', is_flag=True, help="Download data")
@click.option('--reload', is_flag=True, help="Force reload this dashboard")
def load_dashboard(ctx, data_directory, id, projects, download, reload):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]
    nonexistant_projects = [project for project in projects if not _is_project_exist(
        project, es_host, es_port)]

    assert len(
        nonexistant_projects) == 0, f'Projects do not exist: {nonexistant_projects} '

    download_type = "merged" if os.path.exists(os.path.join(
        data_directory, constants.MERGED_DIRECTORYNAME, f"{id}.json")) else "single"

    if download:
        if download_type == "merged":
            _download_libraries_for_merged(id, data_directory)
        elif download_type == "single":
            data_directory = _download_analysis(
                id, data_directory)

    if reload:
        _clean_analysis(id, host=es_host, port=es_port, projects=projects)

    if _is_loaded(id, es_host, es_port):
        [_add_project(project_name, id, es_host, es_port)
         for project_name in projects]

    else:
        if download_type == "merged":
            _load_merged_analysis(id, projects,
                                  data_directory, es_host, es_port)
        elif download_type == "single":
            _load_analysis(id, projects, data_directory, es_host, es_port)


@ main.command()
@ click.argument('dashboard_id')
@ click.pass_context
def clean_analysis(ctx, dashboard_id):
    _clean_analysis(dashboard_id,
                    host=ctx.obj['host'], port=ctx.obj['port'])


@ main.command()
@ click.pass_context
def initialize_db(ctx):
    _initialize_es_indices(host=ctx.obj['host'], port=ctx.obj['port'])


@ main.command()
@ click.argument('project_name')
@ click.pass_context
@ click.option('--dashboard', '-d', 'dashboards', multiple=True, default=[""], help='Dashboard to add to project')
def add_project(ctx, project_name, dashboards):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    assert not _is_project_exist(
        project_name, es_host, es_port), f'Project with name {project_name} already exists'

    if dashboards[0] == "":
        dashboards = []

    unloaded_dashboards = [dashboard_id for dashboard_id in dashboards if not _is_loaded(
        dashboard_id, es_host, es_port)]

    assert len(
        unloaded_dashboards) == 0, f'Dashboards do not exist: {unloaded_dashboards}'

    _add_project(project_name, list(dashboards), es_host, es_port)


def start():
    main(obj={})


if __name__ == '__main__':
    start()
