
# This is a helper file that contains various utils.
# Date formatters, logging facilities and a result parser.


import os
import io
import csv
import logging
from datetime import datetime
from collections import defaultdict
from dateutil.relativedelta import relativedelta


DATE_FORMAT = '%Y-%m-%d'
TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'
DATE_AND_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def raise_critical(error_class, message):
    logging.critical(message)
    raise error_class(message)


def get_previous_results(report, output_folder):
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
        except IOError, e:
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
                row[0] = date
                if report.is_funnel:
                    data[date].append(row)
                else:
                    data[date] = row
        previous_results['header'] = header
        previous_results['data'] = data
    return previous_results


def get_wikis(config):
    if 'wikis_path' not in config:
        raise_critical(KeyError, 'Wikis path is not in config.')
    try:
        wikis_file = io.open(config['wikis_path'], 'r')
    except Exception, e:
        raise_critical(RuntimeError, 'Could not open the wikis file (' + str(e) + ').')
    wikis = []
    for wiki in wikis_file:
        wikis.append(wiki.strip())
    wikis_file.close()
    return wikis


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


def get_increment(period):
    if period == 'hours':
        return relativedelta(hours=1)
    elif period == 'days':
        return relativedelta(days=1)
    elif period == 'weeks':
        return relativedelta(days=7)
    elif period == 'months':
        return relativedelta(months=1)
    else:
        raise ValueError('Period is not valid.')
