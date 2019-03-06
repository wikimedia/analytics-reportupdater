
from reportupdater.report import Report
from unittest import TestCase
from datetime import datetime


class ReportTest(TestCase):

    def setUp(self):
        self.report = Report()
        self.report.key = 'report_test'
        self.report.type = 'sql'
        self.report.granularity = 'days'
        self.report.lag = 0
        self.report.is_funnel = True
        self.report.first_date = datetime(2015, 1, 1)
        self.report.start = datetime(2015, 1, 2)
        self.report.end = datetime(2015, 1, 3)
        self.report.db_key = 'report_test'
        self.report.sql_template = ('SELECT date, value FROM table '
                                    'WHERE date >= {from_timestamp} '
                                    'AND date < {to_timestamp}')
        self.report.script = '/path/to/script'
        self.explode_by = {'wiki': ['enwiki', 'all']}
        self.report.results = {
            'header': ['date', 'value'],
            'data': {
                datetime(2015, 1, 1): ['2015-01-01', '100'],
                datetime(2015, 1, 2): ['2015-01-02', '200']
            }
        }

    def test_str_does_not_raise_error_when_first_date_is_not_expected(self):
        self.report.first_date = None
        str(self.report)
        self.report.first_date = 'string instead of datetime'
        str(self.report)
        self.report.first_date = {}
        str(self.report)

    def test_str_does_not_raise_error_when_start_is_not_expected(self):
        self.report.start = None
        str(self.report)
        self.report.start = 'string instead of datetime'
        str(self.report)
        self.report.start = {}
        str(self.report)

    def test_str_does_not_raise_error_when_end_is_not_expected(self):
        self.report.end = None
        str(self.report)
        self.report.end = 'string instead of datetime'
        str(self.report)
        self.report.end = {}
        str(self.report)

    def test_str_does_not_raise_error_when_results_is_not_expected(self):
        self.report.results = None
        str(self.report)
        self.report.results = 'string instead of dict'
        str(self.report)
        self.report.results = {
            'header': []
            # missing data entry
        }
        str(self.report)
        self.report.results = {
            # missing header entry
            'data': {}
        }
        str(self.report)
        self.report.results = {
            'header': None,
            'data': None
        }
        str(self.report)
