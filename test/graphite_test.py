
import shutil
from reportupdater.writer import Writer
from reportupdater.executor import Executor
from reportupdater.selector import Selector
from reportupdater.reader import Reader
from reportupdater.report import Report
from reportupdater.reportupdater import configure_graphite, load_config
from reportupdater.utils import get_exploded_report_output_path
from unittest import TestCase
from datetime import datetime
from mock import call, MagicMock


class GraphiteTest(TestCase):


    def setUp(self):
        self.config = load_config('test/fixtures/config/graphite_test1.yaml')
        self.config['query_folder'] = 'test/fixtures/config'
        self.config['output_folder'] = 'test/fixtures/output'
        self.config['reruns'] = {}

        self.graphite = configure_graphite(self.config)
        self.graphite_record_stash = self.graphite.record
        self.graphite.record = MagicMock()

        reader = Reader(self.config)
        selector = Selector(reader, self.config)
        executor = Executor(selector, self.config)
        self.writer = Writer(executor, self.config, self.graphite)

        self.report = Report()
        self.report.key = 'graphite_test1'
        self.report.sql_template = 'SOME sql TEMPLATE;'
        self.report.results = {
            'header': ['date', 'value'],
            'data': {
                datetime(2015, 1, 1): [datetime(2015, 1, 1), '1']
            }
        }
        self.report_config = self.config['reports'][self.report.key]
        self.report.graphite = self.report_config['graphite']
        self.report.explode_by = {
            'wiki': 'enwiki',
            'editor': 'wikitext',
        }


    def tearDown(self):
        self.graphite.record = self.graphite_record_stash
        shutil.rmtree('test/fixtures/output/graphite_test1')


    def test_send_new_dates_to_graphite(self):
        old_date = datetime(2015, 1, 1)
        new_date_1 = datetime(2015, 1, 2)
        new_date_2 = datetime(2015, 1, 3)
        new_row_1 = [new_date_1, 1, 2, 3]
        new_row_2 = [new_date_2, 1, 2, 3]
        self.report.granularity = 'days'
        self.report.start = old_date
        self.report.results = {
            'header': ['date', 'val1', 'val2', 'val3'],
            'data': {
                new_date_1: new_row_1,
                new_date_2: new_row_2,
            }
        }
        header, updated_data, new_dates = self.writer.update_results(self.report)

        self.writer.record_to_graphite(self.report, new_dates)
        self.graphite.record.assert_has_calls([
            call('metric_name_one.en.wiki.wikitext', 1, 1420174800.0),
            call('metric_name_two.en.wiki.wikitext', 3, 1420174800.0),
            call('metric_name_one.en.wiki.wikitext', 1, 1420261200.0),
            call('metric_name_two.en.wiki.wikitext', 3, 1420261200.0),
        ], any_order=True)
