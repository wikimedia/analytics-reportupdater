
# This module is in charge of triaging which reports
# must be executed depending on:
#   1. The time that has passed sinnce the last execution
#   2. If the report data is up to date or not
#
# It also divides reports in intervals of one time unit.
# For example, if the report in question has a monthly granularity,
# divides a 3-month report into 3 1-month reports.


import logging
from copy import deepcopy
from datetime import datetime
from dateutil.relativedelta import relativedelta
from reader import Reader
from utils import raise_critical, get_previous_results, get_increment, DATE_FORMAT


class Selector(object):


    def __init__(self, reader, config):
        if not isinstance(reader, Reader):
            raise_critical(ValueError, 'Reader is not valid.')
        if not isinstance(config, dict):
            raise_critical(ValueError, 'Config is not a dict.')
        self.reader = reader
        self.config = config


    def run(self):
        if 'current_exec_time' not in self.config:
            raise_critical(KeyError, 'Current exec time is not in config.')
        now = self.config['current_exec_time']
        if not isinstance(now, datetime):
            raise_critical(ValueError, 'Current exec time is not a date.')

        for report in self.reader.run():
            logging.debug('Triaging "{report}"...'.format(report=str(report)))
            try:
                for exploded_report in self.explode(report):
                    for interval_report in self.get_interval_reports(exploded_report, now):
                        yield interval_report
            except Exception, e:
                message = ('Report "{report_key}" could not be triaged for execution '
                           'because of error: {error}')
                logging.error(message.format(report_key=report.key, error=str(e)))


    def get_interval_reports(self, report, now):
        if 'output_folder' not in self.config:
            raise KeyError('Output folder is not in config.')
        output_folder = self.config['output_folder']
        if not isinstance(output_folder, str):
            raise ValueError('Output folder is not a string.')

        first_date = self.truncate_date(report.first_date, report.granularity)
        lag_increment = relativedelta(seconds=report.lag)
        granularity_increment = get_increment(report.granularity)
        relative_now = now - lag_increment - granularity_increment
        last_date = self.truncate_date(relative_now, report.granularity)
        if report.max_data_points:
            jump_back = get_increment(report.granularity, report.max_data_points - 1)
            first_date = max(first_date, last_date - jump_back)
        previous_results = get_previous_results(
            report, output_folder, self.config['reruns'])
        already_done_dates = previous_results['data'].keys()
        logging.debug('Already done dates: {}'.format(
            [datetime.strftime(d, DATE_FORMAT) for d in sorted(already_done_dates)]
        ))

        for start in self.get_all_start_dates(first_date, last_date, granularity_increment):
            if start not in already_done_dates:
                report_copy = deepcopy(report)
                report_copy.start = start
                report_copy.end = start + granularity_increment
                yield report_copy


    def truncate_date(self, date, period):
        if period == 'hours':
            return date.replace(minute=0, second=0, microsecond=0)
        elif period == 'days':
            return date.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == 'weeks':
            # The week is considered to start on Sunday for convenience,
            # so that the weekly results are already available on Monday.
            passed_weekdays = relativedelta(days=date.isoweekday() % 7)
            return self.truncate_date(date, 'days') - passed_weekdays
        elif period == 'months':
            return date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            raise ValueError('Period is not valid.')


    def get_all_start_dates(self, first_date, current_date, increment):
        if first_date > current_date:
            raise ValueError('First date is greater than current date.')
        if increment.days < 0 or increment.months < 0:
            raise ValueError('Increment is negative.')
        current_start = first_date
        while current_start <= current_date:
            yield current_start
            current_start += increment


    def explode(self, report, visited=set([])):
        placeholders = set(report.explode_by.keys())
        remaining_placeholders = placeholders.difference(visited)

        if len(remaining_placeholders) > 0:  # recursive case
            placeholder = remaining_placeholders.pop()
            values = report.explode_by[placeholder]
            visited.add(placeholder)
            exploded_reports = []
            for value in values:
                report_copy = deepcopy(report)
                report_copy.explode_by[placeholder] = value
                exploded_reports.extend(self.explode(report_copy, visited))
            visited.remove(placeholder)
            return exploded_reports

        else:  # simple case
            return [report]
