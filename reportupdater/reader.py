
# This module implements the first step in the pipeline.
# Reads the report information from the config file.
# For each report section contained in the config,
# creates a Report object, that will be passed
# to the rest of the pipeline.
#
# This step tries to check all possible input type and format
# issues to minimize the impact of a possible config error.


import os
import io
import logging
from datetime import datetime, date
from report import Report
from utils import DATE_FORMAT, raise_critical


class Reader(object):

    def __init__(self, config):
        if not isinstance(config, dict):
            raise_critical(ValueError, 'Config is not a dict.')
        self.config = config

    def run(self):
        if 'reports' not in self.config:
            raise_critical(KeyError, 'Reports is not in config.')
        reports = self.config['reports']
        if not isinstance(reports, dict):
            raise_critical(ValueError, 'Reports is not a dict.')
        for report_key, report_config in reports.iteritems():
            logging.debug('Reading "{report_key}"...'.format(report_key=report_key))
            try:
                report = self.create_report(report_key, report_config)
                yield report
            except Exception, e:
                message = ('Report "{report_key}" could not be read from config '
                           'because of error: {error}')
                logging.error(message.format(report_key=report_key, error=str(e)))

    def create_report(self, report_key, report_config):
        if not isinstance(report_key, str):
            raise TypeError('Report key is not a string.')
        if not isinstance(report_config, dict):
            raise TypeError('Report config is not a dict.')
        if 'query_folder' not in self.config:
            raise KeyError('Query folder is not in config.')
        query_folder = self.config['query_folder']
        if not isinstance(query_folder, str):
            raise ValueError('Query folder is not a string.')
        report = Report()
        report.key = report_key
        report.type = self.get_type(report_config)
        report.granularity = self.get_granularity(report_config)
        report.lag = self.get_lag(report_config)
        report.is_funnel = self.get_is_funnel(report_config)
        report.first_date = self.get_first_date(report_config)
        report.explode_by = self.get_explode_by(report_config, query_folder)
        report.max_data_points = self.get_max_data_points(report_config)
        report.group = self.get_group(report_config)
        executable = self.get_executable(report_config) or report_key
        if report.type == 'sql':
            report.db_key = self.get_db_key(report_config)
            report.sql_template = self.get_sql_template(executable, query_folder)
        elif report.type == 'script':
            report.script = self.get_script(executable, query_folder)
        report.graphite = self.get_graphite(report_config)
        return report

    def get_type(self, report_config):
        report_type = report_config.get('type', 'sql')
        if report_type not in ['sql', 'script']:
            raise ValueError('Report type is not valid.')
        return report_type

    def get_granularity(self, report_config):
        if 'granularity' not in report_config:
            raise KeyError('Report granularity is not specified.')
        granularity = report_config['granularity']
        if granularity not in ['days', 'weeks', 'months']:
            raise ValueError('Report granularity is not valid.')
        return granularity

    def get_lag(self, report_config):
        if 'lag' not in report_config:
            return 0
        lag = report_config['lag']
        if type(lag) != int or lag < 0:
            raise ValueError('Report lag is not valid.')
        return lag

    def get_is_funnel(self, report_config):
        return 'funnel' in report_config and report_config['funnel'] is True

    def get_first_date(self, report_config):
        if 'starts' in report_config:
            first_date = report_config['starts']
            if isinstance(first_date, date):
                first_date = datetime(first_date.year, first_date.month, first_date.day)
            else:
                try:
                    first_date = datetime.strptime(first_date, DATE_FORMAT)
                except TypeError:
                    raise TypeError('Report starts is not a string.')
                except ValueError:
                    raise ValueError('Report starts does not match date format')
            return first_date
        else:
            raise ValueError('Report does not specify starts.')

    def get_db_key(self, report_config):
        if 'db' in report_config:
            db_key = report_config['db']
        elif 'defaults' not in self.config:
            raise KeyError('Defaults is not in config.')
        elif 'db' not in self.config['defaults']:
            raise KeyError('DB default is not in defaults config.')
        else:
            db_key = self.config['defaults']['db']
        if not isinstance(db_key, str):
            raise ValueError('DB key is not a string.')
        return db_key

    def get_sql_template(self, report_key, query_folder):
        sql_template_path = os.path.join(query_folder, report_key + '.sql')
        try:
            with io.open(sql_template_path, encoding='utf-8') as sql_template_file:
                return sql_template_file.read()
        except IOError, e:
            raise IOError('Could not read the SQL template (' + str(e) + ').')

    def get_script(self, report_key, query_folder):
        return os.path.join(query_folder, report_key)

    def get_explode_by(self, report_config, query_folder):
        explode_by = {}
        if 'explode_by' in report_config:
            for placeholder, values_str in report_config['explode_by'].iteritems():
                values = [value.strip() for value in values_str.split(',')]
                if len(values) == 1:
                    explode_path = os.path.join(query_folder, values[0])
                    try:
                        with io.open(explode_path, encoding='utf-8') as explode_file:
                            read_values = [v.strip() for v in explode_file.readlines()]
                        if (len(read_values) > 0):
                            explode_by[placeholder] = read_values
                    except IOError:
                        explode_by[placeholder] = values
                elif len(values) > 1:
                    explode_by[placeholder] = values
        return explode_by

    def get_max_data_points(self, report_config):
        if 'max_data_points' not in report_config:
            return None
        max_data_points = report_config['max_data_points']
        if type(max_data_points) != int or max_data_points < 1:
            raise ValueError('Max data points is not valid.')
        return max_data_points

    def get_executable(self, report_config):
        if 'execute' not in report_config:
            return None
        execute = report_config['execute']
        if not isinstance(execute, str):
            raise TypeError('Execute is not a string.')
        return execute

    def get_graphite(self, report_config):
        if 'graphite' not in report_config:
            return {}
        graphite = report_config['graphite']
        if not isinstance(graphite, dict):
            raise TypeError('Graphite is not a dict.')
        return graphite

    def get_group(self, report_config):
        group = None
        if 'group' in report_config:
            group = report_config['group']
        elif 'defaults' in self.config and 'group' in self.config['defaults']:
            group = self.config['defaults']['group']
        if group is not None and not isinstance(group, str):
            raise ValueError('Group is not a string.')
        return group
