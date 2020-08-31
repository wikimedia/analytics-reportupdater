
# This is the main module of the project.
#
# Its 'run' method will execute the whole pipeline:
#   1. Read the report information from config file
#   2. Select or triage the reports that have to be executed
#   3. Execute those reports against the database
#   4. Write / update the files with the results
#
# In addition to that, this module uses a pid file
# to avoid concurrent execution; blocking instances to run
# when another instance is already running.
#
# Also, it stores and controls the last execution time,
# used for report scheduling in the select step.


import os
import io
import yaml
import logging
from pid import PidFile, PidFileError
from datetime import datetime
from .reader import Reader
from .selector import Selector
from .executor import Executor
from .writer import Writer
from .graphite import Graphite
from .utils import DATE_FORMAT


def run(**kwargs):
    params = get_params(kwargs)
    configure_logging(params)

    try:
        with PidFile(get_pidfile_key(params['query_folder'])):
            logging.info('Starting execution.')

            current_exec_time = utcnow()

            config = load_config(params['config_path'])
            config['current_exec_time'] = current_exec_time
            config['query_folder'] = params['query_folder']
            config['output_folder'] = params['output_folder']
            config['reruns'], rerun_files = read_reruns(params['query_folder'])

            reader = Reader(config)
            selector = Selector(reader, config)
            executor = Executor(selector, config)
            writer = Writer(executor, config, configure_graphite(config))
            writer.run()

            delete_reruns(rerun_files)  # delete rerun files that have been processed
            logging.info('Execution complete.')
    except PidFileError:
        logging.warning('A job with folder {} is already running, exiting successfully.'.format(params['query_folder']))


def get_params(passed_params):
    project_root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
    query_folder = passed_params.pop('query_folder', os.path.join(project_root, 'queries'))
    process_params = {
        'config_path': os.path.join(query_folder, 'config.yaml'),
        'output_folder': os.path.join(project_root, 'output'),
        'log_level': logging.WARNING
    }
    passed_params = {k: v for k, v in list(passed_params.items()) if v is not None}
    process_params.update(passed_params)
    process_params['query_folder'] = query_folder
    return process_params


def configure_logging(params):
    logger = logging.getLogger()
    if 'log_file' in params:
        handler = logging.FileHandler(params['log_file'])
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(params['log_level'])


def get_pidfile_key(query_folder):
    return 'reportupdater-{}'.format(os.path.abspath(query_folder).replace(os.path.sep, '-'))


def load_config(config_path):
    try:
        with io.open(config_path, encoding='utf-8') as config_file:
            return yaml.safe_load(config_file)
    except IOError as e:
        raise IOError('Can not read the config file because of: (' + str(e) + ').')


def read_reruns(query_folder):
    reruns_folder = os.path.join(query_folder, '.reruns')
    if os.path.isdir(reruns_folder):
        try:
            rerun_candidates = os.listdir(reruns_folder)
        except IOError as e:
            raise IOError('Can not read rerun folder because of: (' + str(e) + ').')
        rerun_config, rerun_files = {}, []
        for rerun_candidate in rerun_candidates:
            rerun_path = os.path.join(reruns_folder, rerun_candidate)
            try:
                # Use r+ mode (read and write) to force an error
                # if the file is still being written.
                with io.open(rerun_path, 'r+', encoding='utf-8') as rerun_file:
                    reruns = rerun_file.readlines()
                parse_reruns(reruns, rerun_config)
                rerun_files.append(rerun_path)
            except Exception as e:
                logging.warning(
                    'Rerun file {} could not be parsed and will be ignored.  Error: {}'.format(
                        rerun_path,
                        str(e),
                    )
                )
        return (rerun_config, rerun_files)
    else:
        return ({}, [])


def parse_reruns(lines, rerun_config):
    values = [line.strip() for line in lines]
    start_date = datetime.strptime(values[0], DATE_FORMAT)
    end_date = datetime.strptime(values[1], DATE_FORMAT)
    for report in values[2:]:
        if report not in rerun_config:
            rerun_config[report] = []
        rerun_config[report].append((start_date, end_date))


def delete_reruns(rerun_files):
    for rerun_file in rerun_files:
        try:
            os.remove(rerun_file)
        except IOError:
            logging.warning('Rerun file %s could not be deleted.' % rerun_file)


def configure_graphite(config):
    graphite = None
    if 'graphite' in config:
        # load any lookup dictionaries that Graphite metrics can use
        for key, lookup in list(config['graphite'].get('lookups', {}).items()):
            path = os.path.join(config['query_folder'], lookup)
            config['graphite']['lookups'][key] = load_config(path)
        graphite = Graphite(config)

    return graphite


def utcnow():
    return datetime.utcnow()
