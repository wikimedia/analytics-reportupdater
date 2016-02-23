
# This module is in charge of triaging which reports
# must be executed depending on:
#   1. The time that has passed sinnce the last execution
#   2. If the report data is up to date or not
#
# It also divides timeboxed reports in intervals of one time unit.
# For example, if the report in question has a monthly granularity,
# divides a 3-month report into 3 1-month reports.


import logging
from copy import deepcopy
from datetime import datetime
from dateutil.relativedelta import relativedelta
from reader import Reader
from utils import raise_critical, get_previous_results


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
        if 'last_exec_time' not in self.config:
            raise_critical(KeyError, 'Last exec time is not in config.')
        now = self.config['current_exec_time']
        last_exec_time = self.config['last_exec_time']
        if not isinstance(now, datetime):
            raise_critical(ValueError, 'Current exec time is not a date.')
        if last_exec_time and last_exec_time > now:
            raise_critical(ValueError, 'Last exec time is greater than current exec time.')

        for report in self.reader.run():
            logging.debug('Triaging "{report}"...'.format(report=str(report)))
            try:
                if self.is_time_to_execute(last_exec_time, now, report.frequency):
                    for exploded_report in self.explode(report):
                        if report.is_timeboxed:
                            for interval_report in self.get_interval_reports(exploded_report, now):
                                yield interval_report
                        else:
                            yield exploded_report
            except Exception, e:
                message = ('Report "{report_key}" could not be triaged for execution '
                           'because of error: {error}')
                logging.error(message.format(report_key=report.key, error=str(e)))


    def is_time_to_execute(self, last_exec_time, now, frequency):
        if last_exec_time:
            t1 = self.truncate_date(last_exec_time, frequency)
        else:
            t1 = None
        t2 = self.truncate_date(now, frequency)
        return t1 != t2


    def get_interval_reports(self, report, now):
        if 'output_folder' not in self.config:
            raise KeyError('Output folder is not in config.')
        output_folder = self.config['output_folder']
        if not isinstance(output_folder, str):
            raise ValueError('Output folder is not a string.')

        first_date = self.truncate_date(report.first_date, report.granularity)
        frequency_increment = self.get_increment(report.frequency)
        lag_increment = relativedelta(seconds=report.lag)
        granularity_increment = self.get_increment(report.granularity)
        relative_now = now - frequency_increment - lag_increment
        last_date = self.truncate_date(relative_now, report.granularity)
        previous_results = get_previous_results(report, output_folder)
        already_done_dates = previous_results['data'].keys()

        for start in self.get_all_start_dates(first_date, last_date, granularity_increment):
            if start == last_date or start not in already_done_dates:
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


    def get_increment(self, period):
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
