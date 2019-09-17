
import os
import io
import shutil
from reportupdater.writer import Writer
from reportupdater.executor import Executor
from reportupdater.selector import Selector
from reportupdater.reader import Reader
from reportupdater.report import Report
from reportupdater.utils import get_exploded_report_output_path
from unittest import TestCase
from datetime import datetime
from mock import MagicMock


class WriterTest(TestCase):

    def setUp(self):
        self.config = {
            'output_folder': 'test/fixtures/output',
            'reruns': {}
        }
        reader = Reader(self.config)
        selector = Selector(reader, self.config)
        executor = Executor(selector, self.config)
        self.writer = Writer(executor, self.config)

        self.report = Report()
        self.report.key = 'writer_test'
        self.report.sql_template = 'SOME sql TEMPLATE;'
        self.report.results = {
            'header': ['date', 'value'],
            'data': {
                datetime(2015, 1, 1): [datetime(2015, 1, 1), '1']
            }
        }

        self.io_open_stash = io.open
        self.os_rename_stash = os.rename
        self.paths_to_clean = []

        with open('test/fixtures/output/writer_test_header_change.tsv', 'w') as second_test:
            second_test.write('date\tval1\tval2\tval3\n2015-01-01\t1\t2\t3')

    def tearDown(self):
        try:
            os.remove('test/fixtures/output/writer_test.tsv')
            os.remove('test/fixtures/output/writer_test_header_change.tsv')
        except:
            pass
        io.open = self.io_open_stash
        os.rename = self.os_rename_stash
        for path in self.paths_to_clean:
            try:
                os.remove(path)
            except:
                try:
                    shutil.rmtree(path)
                except:
                    pass

    def test_write_results_when_io_open_raises_error(self):
        io.open = MagicMock(side_effect=Exception())
        header = self.report.results['header']
        data = self.report.results['data']
        output_folder = self.config['output_folder']
        with self.assertRaises(RuntimeError):
            self.writer.write_results(header, data, self.report, output_folder)

    def test_write_results_when_os_rename_raises_error(self):
        os.rename = MagicMock(side_effect=Exception())
        header = self.report.results['header']
        data = self.report.results['data']
        output_folder = self.config['output_folder']
        with self.assertRaises(RuntimeError):
            self.writer.write_results(header, data, self.report, output_folder)

    def test_write_results_when_results_data_is_empty(self):
        header = ['date', 'value']
        data = {}
        output_folder = self.config['output_folder']
        self.writer.write_results(header, data, self.report, output_folder)
        output_path = os.path.join(output_folder, self.report.key + '.tsv')
        self.paths_to_clean.append(output_path)
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output = output_file.read().strip()
        self.assertEqual(output, 'date\tvalue')

    def test_write_results_with_funnel_data(self):
        self.report.is_funnel = True
        header = ['date', 'value']
        data = {
            datetime(2015, 1, 2): [[datetime(2015, 1, 2), 'c'], [datetime(2015, 1, 2), 'd']],
            datetime(2015, 1, 3): [[datetime(2015, 1, 3), 'e']],
            datetime(2015, 1, 1): [[datetime(2015, 1, 1), 'a'], [datetime(2015, 1, 1), 'b']]
        }
        output_folder = self.config['output_folder']
        self.writer.write_results(header, data, self.report, output_folder)
        output_path = os.path.join(output_folder, self.report.key + '.tsv')
        self.paths_to_clean.append(output_path)
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertEqual(len(output_lines), 6)
        self.assertEqual(output_lines[0], 'date\tvalue\n')
        self.assertEqual(output_lines[1], '2015-01-01\ta\n')
        self.assertEqual(output_lines[2], '2015-01-01\tb\n')
        self.assertEqual(output_lines[3], '2015-01-02\tc\n')
        self.assertEqual(output_lines[4], '2015-01-02\td\n')
        self.assertEqual(output_lines[5], '2015-01-03\te\n')

    def test_write_results_with_explode_by(self):
        self.report.explode_by = {
            'wiki': 'enwiki',
            'editor': 'visualeditor'
        }
        header = ['date', 'value']
        data = {datetime(2015, 1, 1): [datetime(2015, 1, 1), 'a']}
        self.writer.write_results(header, data, self.report, self.config['output_folder'])
        output_folder = 'test/fixtures/output/writer_test'
        self.paths_to_clean.append(output_folder)
        output_path = output_folder + '/visualeditor/enwiki.tsv'
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertEqual(len(output_lines), 2)
        self.assertEqual(output_lines[0], 'date\tvalue\n')
        self.assertEqual(output_lines[1], '2015-01-01\ta\n')

    def test_write_results(self):
        header = ['date', 'value']
        data = {
            datetime(2015, 1, 2): [datetime(2015, 1, 2), 'a'],
            datetime(2015, 1, 3): [datetime(2015, 1, 3), 'b'],
            datetime(2015, 1, 1): [datetime(2015, 1, 1), 'c']
        }
        output_folder = self.config['output_folder']
        self.writer.write_results(header, data, self.report, output_folder)
        output_path = os.path.join(output_folder, self.report.key + '.tsv')
        self.paths_to_clean.append(output_path)
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertEqual(len(output_lines), 4)
        self.assertEqual(output_lines[0], 'date\tvalue\n')
        self.assertEqual(output_lines[1], '2015-01-01\tc\n')
        self.assertEqual(output_lines[2], '2015-01-02\ta\n')
        self.assertEqual(output_lines[3], '2015-01-03\tb\n')

    def test_get_exploded_report_output_path(self):
        explode_by = {
            'wiki': 'enwiki',
            'editor': 'visualeditor'
        }
        result = get_exploded_report_output_path(
            self.config['output_folder'], explode_by, 'writer_test')
        expected = self.config['output_folder'] + '/writer_test/visualeditor/enwiki.tsv'
        self.assertEqual(result, expected)

    def test_run_when_previous_results_header_is_empty(self):
        # self.report has no previous results tsv by default setup
        executed = [self.report]
        self.writer.executor.run = MagicMock(return_value=executed)
        self.writer.write_results = MagicMock()
        self.writer.run()
        self.writer.write_results.assert_called_once_with(
            self.report.results['header'],
            self.report.results['data'],
            self.report,
            self.config['output_folder']
        )

    def test_update_results_when_header_has_new_columns(self):
        # see setUp for the fake data written to this report output
        self.report.key = 'writer_test_header_change'
        new_header = ['date', 'val1', 'insert middle', 'val2', 'val3', 'insert after']
        old_date = datetime(2015, 1, 1)
        new_date = datetime(2015, 1, 2)
        new_row = [datetime(2015, 1, 2), 1, 8, 2, 3, 9]
        self.report.results = {
            'header': new_header,
            'data': {new_date: new_row}
        }
        header, updated_data, new_dates = self.writer.update_results(self.report)
        self.assertEqual(header, new_header)
        self.assertEqual(updated_data[new_date], new_row)
        self.assertEqual(updated_data[old_date], [old_date, '1', None, '2', '3', None])
        self.assertEqual(new_dates, [new_date])

    def test_update_results_when_header_has_moved_columns(self):
        # see setUp for the fake data written to this report output
        self.report.key = 'writer_test_header_change'
        new_header = ['date', 'val2', 'val1', 'val3']
        old_date = datetime(2015, 1, 1)
        new_date = datetime(2015, 1, 2)
        new_row = [datetime(2015, 1, 2), 1, 2, 3]
        self.report.results = {
            'header': new_header,
            'data': {new_date: new_row}
        }
        header, updated_data, new_dates = self.writer.update_results(self.report)
        self.assertEqual(header, new_header)
        self.assertEqual(updated_data[new_date], new_row)
        self.assertEqual(updated_data[old_date], [old_date, '2', '1', '3'])
        self.assertEqual(new_dates, [new_date])

    def test_update_results_when_header_has_removed_columns(self):
        # see setUp for the fake data written to this report output
        self.report.key = 'writer_test_header_change'
        new_header = ['date', 'val1', 'val3']
        new_date = datetime(2015, 1, 2)
        new_row = [datetime(2015, 1, 2), 1, 3]
        self.report.results = {
            'header': new_header,
            'data': {new_date: new_row}
        }
        header, updated_data, new_dates = self.writer.update_results(self.report)
        self.assertEqual(header, ['date', 'val1', 'val3', 'val2'])
        self.assertEqual(updated_data[new_date], [datetime(2015, 1, 2), 1, 3, None])
        self.assertEqual(new_dates, [new_date])

    def test_update_results_when_header_has_different_number_of_columns(self):
        # see setUp for the fake data written to this report output
        self.report.key = 'writer_test_header_change'
        new_header = ['date', 'val1', 'val2', 'val3']
        new_date = datetime(2015, 1, 2)
        new_row = [datetime(2015, 1, 2), 1, 2, 3, 'Additional']
        self.report.results = {
            'header': new_header,
            'data': {new_date: new_row}
        }
        with self.assertRaises(ValueError):
            self.writer.update_results(self.report)

    def test_update_results_when_header_has_new_and_moved_columns(self):
        # see setUp for the fake data written to this report output
        self.report.key = 'writer_test_header_change'
        new_header = ['date', 'val2', 'insert middle', 'val1', 'val3', 'insert after']
        old_date = datetime(2015, 1, 1)
        new_date = datetime(2015, 1, 2)
        new_row = [datetime(2015, 1, 2), 2, 8, 1, 3, 9]
        self.report.results = {
            'header': new_header,
            'data': {new_date: new_row}
        }
        header, updated_data, new_dates = self.writer.update_results(self.report)
        self.assertEqual(header, new_header)
        self.assertEqual(updated_data[new_date], new_row)
        self.assertEqual(updated_data[old_date], [old_date, '2', None, '1', '3', None])
        self.assertEqual(new_dates, [new_date])

    def test_update_results_when_max_data_points_is_set(self):
        # see setUp for the fake data written to this report output
        self.report.key = 'writer_test_header_change'
        new_date = datetime(2015, 1, 2)
        new_row = [new_date, 1, 2, 3]
        self.report.max_data_points = 1
        self.report.granularity = 'days'
        self.report.start = new_date
        self.report.results = {
            'header': ['date', 'val1', 'val2', 'val3'],  # no changes
            'data': {new_date: new_row}
        }
        header, updated_data, new_dates = self.writer.update_results(self.report)
        self.assertEqual(len(updated_data), 1)
        self.assertTrue(new_date in updated_data)
        self.assertEqual(updated_data[new_date], new_row)
        self.assertEqual(new_dates, [new_date])

    def test_update_results_computes_new_dates_without_reruns(self):
        self.report.key = 'writer_test_header_change'
        old_date = datetime(2015, 1, 1)
        new_date_1 = datetime(2015, 1, 2)
        new_date_2 = datetime(2015, 1, 3)
        new_row_1 = [new_date_1, 1, 2, 3]
        new_row_2 = [new_date_2, 1, 2, 3]
        self.report.granularity = 'days'
        self.report.start = old_date
        self.report.results = {
            'header': ['date', 'val1', 'val2', 'val3'],  # no changes
            'data': {
                new_date_1: new_row_1,
                new_date_2: new_row_2
            }
        }
        header, updated_data, new_dates = self.writer.update_results(self.report)

        self.assertEqual(sorted(new_dates), [new_date_1, new_date_2])

    def test_get_date_threshold_when_max_data_points_is_not_specified(self):
        date_threshold = self.writer.get_date_threshold(self.report, {})
        self.assertEqual(date_threshold, None)

    def test_get_date_threshold_when_no_previous_data_is_passed(self):
        self.report.max_data_points = 3
        self.report.start = datetime(2015, 1, 1)
        self.report.granularity = 'days'
        expected = datetime(2014, 12, 29)
        result = self.writer.get_date_threshold(self.report, {})
        self.assertEqual(result, expected)

    def test_get_date_threshold(self):
        self.report.max_data_points = 3
        self.report.start = datetime(2015, 1, 1)
        self.report.granularity = 'days'
        previous_data = {
            datetime(2014, 12, 30): ['some', 'data'],
            datetime(2014, 12, 31): ['some', 'data']
        }
        expected = datetime(2014, 12, 29)
        result = self.writer.get_date_threshold(self.report, previous_data)
        self.assertEqual(result, expected)

    def test_run_when_helper_method_raises_error(self):
        executed = [self.report]
        self.writer.executor.run = MagicMock(return_value=executed)
        self.writer.write_results = MagicMock(side_effect=Exception())
        self.writer.run()
        # just checking no error is raised

    def test_run(self):
        self.report.key = 'writer_test2'
        output_path = os.path.join(self.config['output_folder'], self.report.key + '.tsv')
        self.paths_to_clean.append(output_path)
        # Set up previous results.
        # File can not be a permanent fixture, because it is overwritten be the test.
        with io.open(output_path, 'w') as output_file:
            output_file.write(str('date\tvalue\n2015-01-01\ta\n2015-01-02\tb\n'))
        # Set up current results.
        self.report.results['header'] = ['date', 'value']
        self.report.results['data'] = {
            datetime(2015, 1, 1): [datetime(2015, 1, 1), 'a'],
            datetime(2015, 1, 2): [datetime(2015, 1, 2), 'b'],
            datetime(2015, 1, 3): [datetime(2015, 1, 3), 'c']
        }
        executed = [self.report]
        self.writer.executor.run = MagicMock(return_value=executed)
        self.writer.run()
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertEqual(len(output_lines), 4)
        self.assertEqual(output_lines[0], 'date\tvalue\n')
        self.assertEqual(output_lines[1], '2015-01-01\ta\n')
        self.assertEqual(output_lines[2], '2015-01-02\tb\n')
        self.assertEqual(output_lines[3], '2015-01-03\tc\n')
