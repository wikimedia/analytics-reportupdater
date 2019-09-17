
# This is a helper file that contains various utils.
# Date formatters, logging facilities and a result parser.


import os
import io
import csv
import glob
import logging
import dns.resolver
from datetime import datetime
from collections import defaultdict
from dateutil.relativedelta import relativedelta


DATE_FORMAT = '%Y-%m-%d'
TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'
DATE_AND_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
db_mapping = None


def raise_critical(error_class, message):
    logging.critical(message)
    raise error_class(message)


def get_previous_results(report, output_folder, reruns):
    # Reads a report file to get its results
    # and returns them in the expected dict(date->row) format.
    previous_results = {'header': [], 'data': {}}
    if len(report.explode_by) > 0:
        output_path = get_exploded_report_output_path(
            output_folder, report.explode_by, report.key)
    else:
        output_path = os.path.join(output_folder, report.key + '.tsv')
    if os.path.exists(output_path):
        try:
            with io.open(output_path, encoding='utf-8') as output_file:
                rows = list(csv.reader(output_file, delimiter='\t'))
        except IOError as e:
            raise IOError('Could not read the output file (' + str(e) + ').')
        header = []
        if report.is_funnel:
            # If the report is for a funnel visualization,
            # one same date may contain several lines in the tsv.
            # So, all lines for the same date, are listed in the
            # same dict entry under the date key.
            data = defaultdict(list)
        else:
            data = {}
        for row in rows:
            if not header:
                header = row  # skip header
            else:
                try:
                    date = datetime.strptime(row[0], DATE_FORMAT)
                except ValueError:
                    raise ValueError('Output file date does not match date format.')
                if needs_rerun(date, reruns.get(report.key, None)):
                    continue  # Do not list this date so that it is re-run.
                row[0] = date
                if report.is_funnel:
                    data[date].append(row)
                else:
                    data[date] = row
        previous_results['header'] = header
        previous_results['data'] = data
    return previous_results


def needs_rerun(date, rerun_intervals):
    if rerun_intervals is None:
        return False
    for start, end in rerun_intervals:
        if date >= start and date < end:
            return True
    return False


def get_exploded_report_output_path(output_folder, explode_by, report_key):
    output_folder = os.path.join(output_folder, report_key)
    placeholders = sorted(explode_by.keys())
    while len(placeholders) > 1:
        placeholder = placeholders.pop(0)
        value = explode_by[placeholder]
        output_folder = os.path.join(output_folder, value)
    ensure_dir(output_folder)
    last_placeholder = placeholders[0]
    last_value = explode_by[last_placeholder]
    output_path = os.path.join(output_folder, last_value + '.tsv')
    return output_path


def ensure_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def get_increment(period, times=1):
    if period == 'hours':
        return relativedelta(hours=times)
    elif period == 'days':
        return relativedelta(days=times)
    elif period == 'weeks':
        return relativedelta(days=7 * times)
    elif period == 'months':
        return relativedelta(months=times)
    else:
        raise ValueError('Period is not valid.')


def get_mediawiki_host_and_port(db_config, db_name):
    global db_mapping
    use_x1 = db_config.get('use_x1', False)
    mw_config_path = db_config.get('mw_config_path', '/srv/mediawiki-config')

    if not db_mapping:
        db_mapping = get_mediawiki_section_dbname_mapping(mw_config_path, use_x1)
    if not db_mapping:
        raise RuntimeError("No database mapping found at {}. Have you configured correctly the mediawiki-config path?"
                           .format(mw_config_path))
    if db_name == 'staging':
        shard = 'staging'
    elif db_name == 'centralauth':
        # The 'centralauth' db is a special case, not currently
        # listed among the mediawiki-config's dblists. The more automated
        # solution would be to parse db-eqiad.php in mediawiki-config, but it
        # would add more complexity than what's necessary.
        shard = 's7'
    elif use_x1:
        shard = 'x1'
    else:
        try:
            shard = db_mapping[db_name]
        except KeyError:
            raise RuntimeError("The database {} is not listed among the dblist files of the supported sections."
                               .format(db_name))
    answers = dns.resolver.query('_' + shard + '-analytics._tcp.eqiad.wmnet', 'SRV')
    host, port = str(answers[0].target).strip('.'), answers[0].port
    return (host, port)


def get_mediawiki_section_dbname_mapping(mw_config_path, use_x1):
    db_mapping = {}
    if use_x1:
        dblist_section_paths = [mw_config_path.rstrip('/') + '/dblists/all.dblist']
    else:
        dblist_section_paths = glob.glob(mw_config_path.rstrip('/') + '/dblists/s[0-9]*.dblist')
    for dblist_section_path in dblist_section_paths:
        with open(dblist_section_path, 'r') as f:
            for db in f.readlines():
                db_mapping[db.strip()] = dblist_section_path.strip().rstrip('.dblist').split('/')[-1]

    return db_mapping
