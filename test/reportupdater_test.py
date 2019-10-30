import os
import io
import time
import shutil
from reportupdater import reportupdater
from reportupdater.utils import DATE_FORMAT
from test_utils import ConnectionMock
from unittest import TestCase
import mock
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from threading import Thread


@mock.patch('reportupdater.reportupdater.utcnow', return_value=datetime(2015, 5, 3))
class ReportUpdaterTest(TestCase):

    def setUp(self):
        self.config_folder = 'test/fixtures/config'
        self.query_folder = 'test/fixtures/queries'
        self.output_folder = 'test/fixtures/output'
        self.pid_file_path = 'test/fixtures/queries/.reportupdater.pid'
        self.paths_to_clean = [self.pid_file_path]

    def tearDown(self):
        for path in self.paths_to_clean:
            try:
                os.remove(path)
            except:
                try:
                    shutil.rmtree(path)
                except:
                    pass

    def test_when_two_threads_run_reportupdater_in_parallel(self, *_):
        # Mock database methods.
        def fetchall_callback():
            return []
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)

        def connect_with_lag(**kwargs):
            # This makes the connection take some time to execute,
            # thus giving time to the second thread to start.
            # FIXME: Should actively poll for a semaphore from the other threads.
            time.sleep(0.3)
            return connection_mock
        with mock.patch('pymysql.connect', wraps=connect_with_lag):

            # The first thread should execute normally and output the results.
            output_path1 = os.path.join(self.output_folder, 'reportupdater_test1.tsv')
            self.paths_to_clean.extend([output_path1])
            args1 = {
                'config_path': os.path.join(self.config_folder, 'reportupdater_test1.yaml'),
                'query_folder': self.query_folder,
                'output_folder': self.output_folder
            }
            thread1 = Thread(target=reportupdater.run, kwargs=args1)
            thread1.start()

            # The second thread will start when the first thread is still running,
            # so it should be discarded by the pidfile control
            # and no output should be written.
            time.sleep(0.1)
            output_path2 = os.path.join(self.output_folder, 'reportupdater_test2.tsv')
            self.paths_to_clean.extend([output_path2])
            args2 = {
                'config_path': os.path.join(self.config_folder, 'reportupdater_test2.yaml'),
                'query_folder': self.query_folder,
                'output_folder': self.output_folder
            }
            thread2 = Thread(target=reportupdater.run, kwargs=args2)
            thread2.start()

            # wait for the threads to finish and assert results
            thread1.join()
            output_path1 = os.path.join(self.output_folder, 'reportupdater_test1.tsv')
            self.assertTrue(os.path.exists(output_path1))
            thread2.join()
            output_path2 = os.path.join(self.output_folder, 'reportupdater_test2.tsv')
            self.assertFalse(os.path.exists(output_path2))

    def test_hourly_report_without_previous_results(self, *_):
        def fetchall_callback():
            # This method will return a subsequent row with each call.
            try:
                sql_date = self.last_date + relativedelta(days=+1)
                value = self.last_value + 1
            except AttributeError:
                sql_date = date(2015, 1, 1)
                value = 1
            self.last_date = sql_date
            self.last_value = value
            return [[sql_date, str(value)]]
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)
        with mock.patch('pymysql.connect', return_value=connection_mock):

            config_path = os.path.join(self.config_folder, 'reportupdater_test1.yaml')
            output_path = os.path.join(self.output_folder, 'reportupdater_test1.tsv')
            self.paths_to_clean.extend([output_path])
            reportupdater.run(
                config_path=config_path,
                query_folder=self.query_folder,
                output_folder=self.output_folder
            )
            self.assertTrue(os.path.exists(output_path))
            with io.open(output_path, 'r', encoding='utf-8') as output_file:
                output_lines = output_file.readlines()
            self.assertTrue(len(output_lines) > 1)
            header = output_lines.pop(0).strip()
            self.assertEqual(header, 'date\tvalue')
            # Assert that all lines hold subsequent values.
            expected_date = datetime(2015, 1, 1)
            expected_value = 1
            for line in output_lines:
                expected_line = expected_date.strftime(DATE_FORMAT) + '\t' + str(expected_value)
                self.assertEqual(line.strip(), expected_line)
                expected_date += relativedelta(days=+1)
                expected_value += 1

    def test_hourly_funnel_report_without_previous_results(self, *_):
        def fetchall_callback():
            # This method will return a subsequent row with each call.
            try:
                sql_date = self.last_date + relativedelta(days=+1)
            except AttributeError:
                sql_date = date(2015, 1, 1)
            self.last_date = sql_date
            return [
                [sql_date, '1'],
                [sql_date, '2'],
                [sql_date, '3']
            ]
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)
        with mock.patch('pymysql.connect', return_value=connection_mock):

            config_path = os.path.join(self.config_folder, 'reportupdater_test3.yaml')
            output_path = os.path.join(self.output_folder, 'reportupdater_test3.tsv')
            self.paths_to_clean.extend([output_path])
            reportupdater.run(
                config_path=config_path,
                query_folder=self.query_folder,
                output_folder=self.output_folder
            )
            self.assertTrue(os.path.exists(output_path))
            with io.open(output_path, 'r', encoding='utf-8') as output_file:
                output_lines = output_file.readlines()
            self.assertTrue(len(output_lines) > 1)
            header = output_lines.pop(0).strip()
            self.assertEqual(header, 'date\tvalue')
            # Assert that all lines hold subsequent values.
            expected_date = datetime(2015, 1, 1)
            expected_value = 1
            for line in output_lines:
                expected_line = expected_date.strftime(DATE_FORMAT) + '\t' + str(expected_value)
                self.assertEqual(line.strip(), expected_line)
                if expected_value < 3:
                    expected_value += 1
                else:
                    expected_date += relativedelta(days=+1)
                    expected_value = 1

    def test_daily_report_with_previous_results(self, *_):
        def fetchall_callback():
            # This method will return a subsequent row with each call.
            try:
                sql_date = self.last_date + relativedelta(months=+1)
                value = self.last_value + 1
            except AttributeError:
                # Starts at Mar, Jan and Feb are in previous results
                sql_date = datetime(2015, 3, 1)
                value = 3
            self.last_date = sql_date
            self.last_value = value
            return [[sql_date, str(value)]]
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)
        with mock.patch('pymysql.connect', return_value=connection_mock):

            config_path = os.path.join(self.config_folder, 'reportupdater_test2.yaml')
            output_path = os.path.join(self.output_folder, 'reportupdater_test2.tsv')
            with io.open(output_path, 'w') as output_file:
                output_file.write(str('date\tvalue\n2015-01-01\t1\n2015-02-01\t2\n'))
            self.paths_to_clean.extend([output_path])
            reportupdater.run(
                config_path=config_path,
                query_folder=self.query_folder,
                output_folder=self.output_folder
            )
            self.assertTrue(os.path.exists(output_path))
            with io.open(output_path, 'r', encoding='utf-8') as output_file:
                output_lines = output_file.readlines()
            self.assertTrue(len(output_lines) > 1)
            header = output_lines.pop(0).strip()
            self.assertEqual(header, 'date\tvalue')
            # Assert that all lines hold subsequent values.
            expected_date = datetime(2015, 1, 1)
            expected_value = 1
            for line in output_lines:
                expected_line = expected_date.strftime(DATE_FORMAT) + '\t' + str(expected_value)
                self.assertEqual(line.strip(), expected_line)
                expected_date += relativedelta(months=+1)
                expected_value += 1

    def test_daily_report_without_previous_results_with_explode_by(self, *_):
        def fetchall_callback():
            return [[datetime(2015, 1, 1), str(1)]]
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)
        with mock.patch('pymysql.connect', return_value=connection_mock):

            config_path = os.path.join(self.config_folder, 'reportupdater_test4.yaml')
            reportupdater.run(
                config_path=config_path,
                query_folder=self.query_folder,
                output_folder=self.output_folder
            )

            output_folder = os.path.join(self.output_folder, 'reportupdater_test4')
            self.paths_to_clean.extend([output_folder])

            output_filenames = [
                'visualeditor/wiki1.tsv',
                'visualeditor/wiki2.tsv',
                'visualeditor/wiki3.tsv',
                'wikitext/wiki1.tsv',
                'wikitext/wiki2.tsv',
                'wikitext/wiki3.tsv',
            ]
            for output_filename in output_filenames:
                output_path = os.path.join(output_folder, output_filename)
                self.assertTrue(os.path.exists(output_path))
                with io.open(output_path, 'r', encoding='utf-8') as output_file:
                    output_lines = output_file.readlines()
                self.assertEqual(len(output_lines), 2)
                self.assertEqual(output_lines[0], 'date\tvalue\n')
                self.assertEqual(output_lines[1], '2015-01-01\t1\n')

    def test_daily_script_report_without_previous_results(self, *_):
        config_path = os.path.join(self.config_folder, 'reportupdater_test5.yaml')
        reportupdater.run(
            config_path=config_path,
            query_folder=self.query_folder,
            output_folder=self.output_folder
        )
        output_path = os.path.join(self.output_folder, 'reportupdater_test5.tsv')
        self.paths_to_clean.extend([output_path])

        self.assertTrue(os.path.exists(output_path))
        with io.open(output_path, 'r', encoding='utf-8') as output_file:
            output_lines = output_file.readlines()
        self.assertTrue(len(output_lines) > 1)
        header = output_lines.pop(0).strip()
        self.assertEqual(header, 'date\tvalue')
        # Assert that all lines hold subsequent dates.
        expected_date = datetime(2015, 1, 1)
        for line in output_lines:
            date_str, value = line.strip().split('\t')
            expected_date_str = expected_date.strftime(DATE_FORMAT)
            self.assertEqual(date_str, expected_date_str)
            self.assertEqual(type(value), str)
            expected_date += relativedelta(days=+1)

    def test_daily_report_with_previous_results_and_reruns(self, mock_utcnow):
        mock_utcnow.return_value = datetime(2016, 1, 8)

        def fetchall_callback():
            # This method will return a subsequent row with each call.
            try:
                sql_date = self.last_date + relativedelta(days=+1)
                value = self.last_value + 1
            except AttributeError:
                # Starts at Mar, Jan and Feb are in previous results
                sql_date = datetime(2016, 1, 1)
                value = 1
            self.last_date = sql_date
            self.last_value = value
            return [[sql_date, str(value)]]
        header = ['date', 'value']
        connection_mock = ConnectionMock(None, fetchall_callback, header)
        with mock.patch('pymysql.connect', return_value=connection_mock):

            config_path = os.path.join(self.config_folder, 'reportupdater_test6.yaml')
            output_path = os.path.join(self.output_folder, 'reportupdater_test6.tsv')
            with io.open(output_path, 'w') as output_file:
                output_file.write(str(
                    'date\tvalue\n'
                    '2016-01-01\t1\n'
                    '2016-01-02\ta\n'  # Note irregular result, this will be overwritten.
                    '2016-01-03\t3\n'
                    '2016-01-04\tb\n'  # Note irregular result, this will be overwritten.
                    '2016-01-05\t5\n'
                ))
            self.paths_to_clean.extend([output_path])

            # Build rerun files.
            rerun_folder = os.path.join(self.query_folder, '.reruns')
            os.makedirs(rerun_folder)
            rerun_path1 = os.path.join(rerun_folder, 'reportupdater_test6.1')
            with io.open(rerun_path1, 'w') as rerun_file1:
                rerun_file1.write(str(
                    '2016-01-02\n'
                    '2016-01-03\n'
                    'reportupdater_test6\n'
                ))
            rerun_path2 = os.path.join(rerun_folder, 'reportupdater_test6.2')
            with io.open(rerun_path2, 'w') as rerun_file2:
                rerun_file2.write(str(
                    '2016-01-04\n'
                    '2016-01-05\n'
                    'reportupdater_test6\n'
                ))
            self.paths_to_clean.extend([rerun_folder])

            reportupdater.run(
                config_path=config_path,
                query_folder=self.query_folder,
                output_folder=self.output_folder
            )
            self.assertTrue(os.path.exists(output_path))
            with io.open(output_path, 'r', encoding='utf-8') as output_file:
                output_lines = output_file.readlines()
            self.assertTrue(len(output_lines) > 1)
            header = output_lines.pop(0).strip()
            self.assertEqual(header, 'date\tvalue')
            # Assert that all lines hold subsequent values.
            expected_date = datetime(2016, 1, 1)
            expected_value = 1
            for line in output_lines:
                expected_line = expected_date.strftime(DATE_FORMAT) + '\t' + str(expected_value)
                self.assertEqual(line.strip(), expected_line)
                expected_date += relativedelta(days=+1)
                expected_value += 1
