import click
import logging
import logging.handlers
import os

from alhena.alhena_loader import load_analysis as _load_analysis
from alhena.alhena_data import download_analysis as _download_analysis
from alhena.elasticsearch import clean_analysis as _clean_analysis


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
@click.option('--reload', is_flag=True, help="Force reload this dashboard")
def load_analysis(ctx, data_directory, id, reload):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    if reload:
        _clean_analysis(id, host=es_host, port=es_port)

    _load_analysis( id, data_directory, es_host, es_port)


@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--id', help="ID of dashboard", required=True)
@click.option('--sample_id')
@click.option('--library_id')
@click.option('--description')
@click.option('--download', is_flag=True, help="Download data")
@click.option('--reload', is_flag=True, help="Force reload this dashboard")
def load_analysis_shah(ctx, data_directory, id, sample_id, library_id, description, download, reload):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]
    if download:
        data_directory = _download_analysis(id, data_directory, sample_id, library_id, description)

    if reload:
        _clean_analysis(id, host=es_host, port=es_port)

    _load_analysis( id, data_directory, es_host, es_port)



@main.command()
@click.argument('dashboard_id')
@click.pass_context
def clean_analysis(ctx, dashboard_id):
    _clean_analysis(dashboard_id,
                    host=ctx.obj['host'], port=ctx.obj['port'])



def start():
    main(obj={})


if __name__ == '__main__':
    start()
