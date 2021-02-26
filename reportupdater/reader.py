
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
from .report import Report
from .utils import DATE_FORMAT, raise_critical


class NoDefaultValue:
    pass


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
        for report_key, report_config in list(reports.items()):
            logging.debug('Reading "{report_key}"...'.format(report_key=report_key))
            try:
                report = self.create_report(report_key, report_config)
                yield report
            except Exception as e:
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

    def get_value(self, key, report_config, global_default):
        defaults = self.config.get('defaults', {})
        if key in report_config:
            value = report_config[key]
        elif key in defaults:
            value = defaults[key]
        elif not isinstance(global_default, NoDefaultValue):
            value = global_default
        else:
            raise KeyError('Key {} must be specified in defaults or report config {}'.format(key, report_config))
        return value

    def get_type(self, report_config):
        report_type = self.get_value('type', report_config, 'sql')
        if report_type not in ['sql', 'script']:
            raise ValueError('Report type is not valid.')
        return report_type

    def get_granularity(self, report_config):
        granularity = self.get_value('granularity', report_config, NoDefaultValue())
        if granularity not in ['days', 'weeks', 'months']:
            raise ValueError('Report granularity is not valid.')
        return granularity

    def get_lag(self, report_config):
        lag = self.get_value('lag', report_config, 0)
        if type(lag) != int or lag < 0:
            raise ValueError('Report lag is not valid.')
        return lag

    def get_first_date(self, report_config):
        first_date = self.get_value('starts', report_config, NoDefaultValue())
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

    def get_db_key(self, report_config):
        db_key = self.get_value('db', report_config, NoDefaultValue())
        if not isinstance(db_key, str):
            raise ValueError('DB key is not a string.')
        return db_key

    def get_sql_template(self, report_key, query_folder):
        sql_template_path = os.path.join(query_folder, report_key + '.sql')
        try:
            with io.open(sql_template_path, encoding='utf-8') as sql_template_file:
                return sql_template_file.read()
        except IOError as e:
            raise IOError('Could not read the SQL template (' + str(e) + ').')

    def get_script(self, report_key, query_folder):
        return os.path.join(query_folder, report_key)

    def get_explode_by(self, report_config, query_folder):
        explode_by_value = self.get_value('explode_by', report_config, {})
        explode_by = {}
        for placeholder, values_str in list(explode_by_value.items()):
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
        max_data_points = self.get_value('max_data_points', report_config, None)
        if max_data_points is not None:
            if type(max_data_points) != int or max_data_points < 1:
                raise ValueError('Max data points is not valid.')
        return max_data_points

    def get_executable(self, report_config):
        execute = self.get_value('execute', report_config, None)
        if execute is not None:
            if not isinstance(execute, str):
                raise TypeError('Execute is not a string.')
        return execute

    def get_graphite(self, report_config):
        graphite = self.get_value('graphite', report_config, {})
        if not isinstance(graphite, dict):
            raise TypeError('Graphite is not a dict.')
        return graphite

    def get_group(self, report_config):
        group = self.get_value('group', report_config, None)
        if group is not None:
            if not isinstance(group, str):
                raise ValueError('Group is not a string.')
        return group
