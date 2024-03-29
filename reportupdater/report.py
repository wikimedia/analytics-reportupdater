
# This module implements the report object that serves as
# communication unit between the several pipeline layers.
# It holds all the information referent to a report,
# such as granularity, start and end dates and results.
# It is not intended to hold any logic.


import re
from datetime import datetime
from .utils import DATE_FORMAT


class Report(object):

    def __init__(self):
        self.key = None
        self.type = None
        self.granularity = None
        self.lag = 0
        self.first_date = None
        self.start = None
        self.end = None
        self.db_key = None
        self.hql_template = None
        self.sql_template = None
        self.script = None
        self.explode_by = {}
        self.max_data_points = None
        self.graphite = {}
        self.results = {'header': [], 'data': {}}
        self.group = None

    def __str__(self):
        return (
            '<Report' +
            ' key=' + str(self.key) +
            ' type=' + str(self.type) +
            ' granularity=' + str(self.granularity) +
            ' lag=' + str(self.lag) +
            ' first_date=' + self.format_date(self.first_date) +
            ' start=' + self.format_date(self.start) +
            ' end=' + self.format_date(self.end) +
            ' db_key=' + str(self.db_key) +
            ' hql_template=' + self.format_template(self.hql_template) +
            ' sql_template=' + self.format_template(self.sql_template) +
            ' script=' + str(self.script) +
            ' explode_by=' + str(self.explode_by) +
            ' max_data_points=' + str(self.max_data_points) +
            ' graphite=' + str(self.graphite) +
            ' results=' + self.format_results(self.results) +
            ' group=' + str(self.group) +
            '>'
        )

    def format_date(self, to_format):
        if to_format:
            if isinstance(to_format, datetime):
                return to_format.strftime(DATE_FORMAT)
            else:
                return 'invalid date'
        else:
            return str(None)

    def format_results(self, to_format):
        if not isinstance(to_format, dict):
            return 'invalid results'
        header = str(to_format.get('header', 'invalid header'))
        data = to_format.get('data', None)
        if isinstance(data, dict):
            data_lines = str(len(data)) + ' rows'
        else:
            data_lines = 'invalid data'
        return str({'header': header, 'data': data_lines})

    def format_template(self, to_format):
        if to_format is None:
            return str(None)
        template = re.sub(r'\s+', ' ', to_format).strip()
        if len(template) > 100:
            template = template[0:100] + '...'
        return template
