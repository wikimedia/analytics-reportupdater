#!/usr/bin/python

import os
import io
import sys
import time
import yaml
import logging
import argparse
from reportupdater import reportupdater
from datetime import datetime


DATE_FORMAT = '%Y-%m-%d'


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=('Mark reports to be re-run for a given date range.')
    )
    parser.add_argument(
        'query_folder',
        help='Folder with *.sql files and scripts.'
    )
    parser.add_argument(
        'start_date',
        help='Start of the date range to be rerun (YYYY-MM-DD, inclusive).'
    )
    parser.add_argument(
        'end_date',
        help='End of the date range to be rerun (YYYY-MM-DD, exclusive).'
    )
    parser.add_argument(
        '--config-path',
        help='Yaml configuration file. Default: <query_folder>/config.yaml.'
    )
    parser.add_argument(
        '-r',
        '--report',
        action='append',
        help=(
            'Report to be re-run. Several reports can be specified like this. '
            'If none is specified, all reports listed in the config file are '
            'marked for re-run.'
        )
    )
    return vars(parser.parse_args())


def critical(message):
    print('ERROR: ' + message)
    sys.exit(1)


def parse_date(args, arg_name):
    try:
        return datetime.strptime(args[arg_name], DATE_FORMAT)
    except ValueError:
        critical('Invalid %s.' % arg_name)


def format_date(d):
    return unicode(d.strftime(DATE_FORMAT)) + u'\n'


def format_report(r):
    return unicode(r) + u'\n'


def main():
    args = parse_arguments()

    # Check dates.
    start_date = parse_date(args, 'start_date')
    end_date = parse_date(args, 'end_date')
    if start_date >= end_date:
        critical('start_date is greater than or equal to end_date.')
    today = datetime.today()
    if end_date > today:
        critical('end_date is greater than today.')

    # Check query folder.
    query_folder = args['query_folder']
    if not os.path.isdir(query_folder):
        critical('Invalid query_folder.')
    
    # Check config.
    config_path = args['config_path']
    if config_path is None:
        config_path = os.path.join(query_folder, 'config.yaml')
    try:
        with io.open(config_path, encoding='utf-8') as config_file:
            config = yaml.load(config_file)
    except IOError:
        critical('Cannot read the config file.')

    # Check reports.
    reports = args['report']
    if 'reports' not in config:
        critical('Cannot find report section in config file.')
    reports_config = config['reports']
    if type(reports_config) != dict:
        critical('Invalid report section in config file.')
    if reports is None:
        reports = reports_config.keys()
    for report in reports:
        if report not in reports_config:
            critical('Report %s is not listed in config file.' % report)
        try:
            first_date = datetime.combine(
                reports_config[report]['starts'],
                datetime.min.time()
            )
        except Exception:
            critical('Cannot parse starts field from %s config.' % report)
        if first_date >= end_date:
            critical('Report %s starts after the specified date range.' % report)

    # Create rerun file.
    reruns_folder = os.path.join(query_folder, '.reruns')
    if not os.path.exists(reruns_folder):
        try:
            os.makedirs(reruns_folder)
        except IOError:
            critical('Could not create reruns folder.')
    rerun_path = os.path.join(reruns_folder, str(int(time.time() * 1000)))
    try:
        with io.open(rerun_path, 'w', encoding='utf-8') as rerun_file:
            rerun_file.writelines(
                [format_date(start_date), format_date(end_date)] +
                map(format_report, reports)
            )
    except IOError:
        critical('Could not write rerun file.')

    print('Reports successfully marked to be re-run.')


if __name__ == '__main__':
    main()
