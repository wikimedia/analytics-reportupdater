
# This module executes the report sql template
# instantiated with the time values passed by the selector.
# It handles the connection with the database,
# formats the results data and stores it inside the report object.


import MySQLdb
import logging
import subprocess
import csv
from datetime import datetime, date
from selector import Selector
from collections import defaultdict
from utils import TIMESTAMP_FORMAT, DATE_FORMAT, raise_critical


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
        if 'databases' not in self.config:
            raise_critical(KeyError, 'Databases is not in config.')
        if not isinstance(self.config['databases'], dict):
            raise_critical(ValueError, 'Databases is not a dict.')
        try:
            sql_query = self.instantiate_sql(report)
            logging.debug(sql_query)
            if report.db_key not in self.connections:
                self.connections[report.db_key] = self.create_connection(report.db_key)
            connection = self.connections[report.db_key]
            report.results = self.execute_sql(sql_query, connection, report.is_funnel)
            return True
        except Exception, e:
            message = ('Report "{report_key}" could not be executed '
                       'because of error: {error}')
            logging.error(message.format(report_key=report.key, error=str(e)))
            return False


    def instantiate_sql(self, report):
        values = {}
        if report.is_timeboxed:
            values['from_timestamp'] = report.start.strftime(TIMESTAMP_FORMAT)
            values['to_timestamp'] = report.end.strftime(TIMESTAMP_FORMAT)
        values.update(report.explode_by)
        try:
            return report.sql_template.format(**values)
        except KeyError:
            raise ValueError('SQL template contains unknown placeholders.')


    def create_connection(self, db_key):
        databases = self.config['databases']
        if db_key not in databases:
            raise KeyError('DB key is not in config databases.')
        db_config = databases[db_key]
        if not isinstance(db_config, dict):
            raise ValueError('DB config is not a dict.')

        if 'host' not in db_config:
            raise KeyError('Host is not in DB config.')
        if 'port' not in db_config:
            raise KeyError('Port is not in DB config.')
        if 'creds_file' not in db_config:
            raise KeyError('Creds file is not in DB config.')
        if 'db' not in db_config:
            raise KeyError('DB name is not in DB config.')

        db_host = db_config['host']
        db_port = db_config['port']
        db_creds_file = db_config['creds_file']
        db_name = db_config['db']

        if not isinstance(db_host, str):
            raise ValueError('Host is not a string.')
        if not isinstance(db_port, int):
            raise ValueError('Port is not an integer.')
        if not isinstance(db_creds_file, str):
            raise ValueError('Creds file is not a string.')
        if not isinstance(db_name, str):
            raise ValueError('DB name is not a string.')
        try:
            return MySQLdb.connect(
                host=db_host,
                port=db_port,
                read_default_file=db_creds_file,
                db=db_name,
                charset='utf8',
                use_unicode=True
            )
        except Exception, e:
            raise RuntimeError('MySQLdb can not connect to database (' + str(e) + ').')


    def execute_sql(self, sql_query, connection, is_funnel=False):
        cursor = connection.cursor()
        try:
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            header = [field[0] for field in cursor.description]
        except Exception, e:
            raise RuntimeError('MySQLdb can not execute query (' + str(e) + ').')
        finally:
            cursor.close()
        if is_funnel:
            data = defaultdict(list)
        else:
            data = {}
        for row in rows:
            sql_date = row[0]
            if not isinstance(sql_date, date):
                raise ValueError('Query results do not have date values in first column.')
            # normalize to datetime
            row = list(row)  # make row item assignable
            row[0] = datetime(sql_date.year, sql_date.month, sql_date.day, 0, 0, 0, 0)
            if is_funnel:
                data[row[0]].append(row)
            else:
                data[row[0]] = row
        return {'header': header, 'data': data}


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
        # execute the script, store its output in a pipe
        try:
            process = subprocess.Popen(parameters, stdout=subprocess.PIPE)
        except OSError, e:
            message = ('Report "{report_key}" could not be executed '
                       'because of error: {error}')
            logging.error(message.format(report_key=report.key, error=str(e)))
            return False
        # parse the results into the report object
        header = None
        if report.is_funnel:
            data = defaultdict(list)
        else:
            data = {}
        tsv_reader = csv.reader(process.stdout, delimiter='\t')
        for row in tsv_reader:
            if header is None:  # first row
                header = row
            else:  # other rows
                try:
                    row[0] = datetime.strptime(row[0], DATE_FORMAT)
                except ValueError:
                    logging.error('Query results do not have date values in first column.')
                    return False
                if report.is_funnel:
                    data[row[0]].append(row)
                else:
                    data[row[0]] = row
        report.results = {'header': header, 'data': data}
        return True
