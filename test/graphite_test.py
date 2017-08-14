
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
import time


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


    def tearDown(self):
        self.graphite.record = self.graphite_record_stash
        shutil.rmtree('test/fixtures/output/graphite_test1')


    def test_send_new_dates_to_graphite(self):
        self.report = Report()
        self.report.key = 'graphite_test1'
        self.report.granularity = 'days'
        self.report.graphite = self.config['reports'][self.report.key]['graphite']
        self.report.explode_by = {
            'wiki': 'enwiki',
            'editor': 'wikitext',
        }
        self.report.start = datetime(2015, 1, 1)
        self.report.results = {
            'header': ['date', 'val1', 'val2', 'val3'],
            'data': {
                datetime(2015, 1, 2): [datetime(2015, 1, 2), 1, 2, 3],
                datetime(2015, 1, 3): [datetime(2015, 1, 3), 1, 2, 3],
            }
        }

        header, updated_data, new_dates = self.writer.update_results(self.report)
        self.writer.record_to_graphite(self.report, new_dates)

        expected_date1 = time.mktime(datetime(2015, 1, 2).timetuple())
        expected_date2 = time.mktime(datetime(2015, 1, 3).timetuple())
        self.graphite.record.assert_has_calls([
            call('metric_name_one.en.wiki.wikitext', 1, expected_date1),
            call('metric_name_two.en.wiki.wikitext', 3, expected_date1),
            call('metric_name_one.en.wiki.wikitext', 1, expected_date2),
            call('metric_name_two.en.wiki.wikitext', 3, expected_date2),
        ], any_order=True)
