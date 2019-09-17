
# This module executes the report sql template
# instantiated with the time values passed by the selector.
# It handles the connection with the database,
# formats the results data and stores it inside the report object.


import pymysql
import logging
import subprocess
import csv
import os
from copy import copy
from datetime import datetime, date
from .selector import Selector
from collections import defaultdict
from .utils import TIMESTAMP_FORMAT, DATE_FORMAT, raise_critical, get_mediawiki_host_and_port


class Executor(object):

    def __init__(self, selector, config):
        if not isinstance(selector, Selector):
            raise_critical(ValueError, 'Selector is not valid.')
        if not isinstance(config, dict):
            raise_critical(ValueError, 'Config is not a dict.')
        self.selector = selector
        self.config = config
        self.connections = {}

    def run(self):
        for report in self.selector.run():
            logging.info('Executing "{report}"...'.format(report=str(report)))
            if report.type == 'sql':
                if self.execute_sql_report(report):
                    yield report
            elif report.type == 'script':
                if self.execute_script_report(report):
                    yield report

    def execute_sql_report(self, report):
        # Get connection key to allow for connection caching,
        # depending on whether dynamic mediawiki dbs are used.
        db_config = self.config['databases'][report.db_key]
        if 'auto_find_db_shard' in db_config:
            if 'wiki_db_placeholder' in db_config:
                db_name = report.explode_by[db_config['wiki_db_placeholder']]
            else:
                db_name = db_config['db']
            connection_key = 'mediawiki_dbs_' + db_name
        else:
            db_name = db_config['db']
            connection_key = report.db_key

        try:
            sql_query = self.instantiate_sql(report)
            logging.debug(sql_query)
            if connection_key not in self.connections:
                self.connections[connection_key] = self.create_connection(db_config, db_name)
            connection = self.connections[connection_key]
            header, data = self.execute_sql(sql_query, connection)
            report.results = self.normalize_results(report, header, data)
            return True
        except Exception as e:
            message = ('Report "{report_key}" could not be executed '
                       'because of error: {error}')
            logging.exception(message.format(report_key=report.key, error=str(e)))
            return False

    def instantiate_sql(self, report):
        values = {
            'from_timestamp': report.start.strftime(TIMESTAMP_FORMAT),
            'to_timestamp': report.end.strftime(TIMESTAMP_FORMAT)
        }
        values.update(report.explode_by)
        try:
            return report.sql_template.format(**values)
        except KeyError:
            raise ValueError('SQL template contains unknown placeholders.')

    def create_connection(self, db_config, db_name):
        if 'auto_find_db_shard' in db_config:
            db_host, db_port = get_mediawiki_host_and_port(db_config, db_name)
        else:
            db_host = db_config['host']
            db_port = db_config['port']
        try:
            return pymysql.connect(
                host=db_host,
                port=db_port,
                read_default_file=db_config['creds_file'],
                db=db_name,
                autocommit=True,
                charset='utf8',
                use_unicode=True
            )
        except Exception as e:
            raise RuntimeError('pymysql can not connect to database (' + str(e) + ').')

    def execute_sql(self, sql_query, connection):
        cursor = connection.cursor()
        try:
            cursor.execute(sql_query)
            data = cursor.fetchall()
            header = [field[0] for field in cursor.description]
        except Exception as e:
            raise RuntimeError('pymysql can not execute query (' + str(e) + ').')
        finally:
            cursor.close()
        return header, data

    def execute_script_report(self, report):
        # prepare parameters for the call
        parameters = [
            report.script,
            report.start.strftime(DATE_FORMAT),
            report.end.strftime(DATE_FORMAT)
        ]
        for dimension in sorted(report.explode_by.keys()):
            value = report.explode_by[dimension]
            parameters.append(value)
        # if a script optionally uses this last parameter
        # it will be the absolute path to itself
        # NOTE: wouldn't this be available to the script anyway?
        parameters.append(os.path.dirname(report.script))
        try:
            # Execute the script, parse the results and normalize them.
            process = subprocess.Popen(parameters, stdout=subprocess.PIPE)
            stdout, _ = process.communicate()
            tsv_reader = csv.reader(stdout.decode().splitlines(), delimiter='\t')
            report.results = self.normalize_results(report, None, tsv_reader)
        except Exception as e:
            message = ('Report "{report_key}" could not be executed '
                       'because of error: {error}')
            logging.exception(message.format(report_key=report.key, error=str(e)))
            return False
        return True

    def normalize_results(self, report, header, data):
        normalized_header = copy(header)
        normalized_data = defaultdict(list) if report.is_funnel else {}

        for row in data:
            # If the header was not explicitly passed, use the first row.
            if normalized_header is None:
                normalized_header = row
                continue

            # Parse the date in the first column.
            raw_date = row[0]
            if isinstance(raw_date, date):
                normalized_date = datetime(raw_date.year, raw_date.month,
                                           raw_date.day, 0, 0, 0, 0)
            elif isinstance(raw_date, str):
                try:
                    normalized_date = datetime.strptime(raw_date, DATE_FORMAT)
                except:
                    raise ValueError('Could not parse date from results.')
            else:
                raise ValueError('Results do not have dates in first column.')

            # Build the normalized data.
            normalized_row = [normalized_date] + list(row[1:])
            if report.is_funnel:
                normalized_data[normalized_date].append(normalized_row)
            else:
                normalized_data[normalized_date] = normalized_row

        # If there's no data, store a row with null values to avoid recomputation.
        if len(normalized_data) == 0:
            empty_row = [report.start] + [None] * (len(normalized_header) - 1)
            normalized_data[report.start] = [empty_row] if report.is_funnel else empty_row

        return {'header': normalized_header, 'data': normalized_data}
