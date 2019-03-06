
from reportupdater.executor import Executor
from reportupdater.selector import Selector
from reportupdater.reader import Reader
from reportupdater.report import Report
from reportupdater.utils import TIMESTAMP_FORMAT
from test_utils import ConnectionMock
from reportupdater.utils import DATE_FORMAT
from unittest import TestCase
from mock import MagicMock
from datetime import datetime, date
import pymysql
import subprocess


class ExecutorTest(TestCase):

    def setUp(self):
        self.db_key = 'executor_test'
        self.db_config = {
            'host': 'some.host',
            'port': 12345,
            'creds_file': '/some/creds/file',
            'db': 'database'
        }
        self.config = {
            'databases': {
                self.db_key: self.db_config
            },
            'query_folder': 'test/fixtures/queries'
        }
        reader = Reader(self.config)
        selector = Selector(reader, self.config)
        self.executor = Executor(selector, self.config)

        self.report = Report()
        self.report.type = 'sql'
        self.report.script = '/some/path'
        self.report.start = datetime(2015, 1, 1)
        self.report.end = datetime(2015, 1, 2)
        self.report.db_key = self.db_key
        self.report.sql_template = ('SELECT date, value FROM table '
                                    'WHERE date >= {from_timestamp} '
                                    'AND date < {to_timestamp};')

    def test_instantiate_sql_when_format_raises_error(self):
        self.report.sql_template = 'SOME sql WITH AN {unknown} placeholder;'
        with self.assertRaises(ValueError):
            self.executor.instantiate_sql(self.report)

    def test_instantiate_sql(self):
        result = self.executor.instantiate_sql(self.report)
        expected = self.report.sql_template.format(
            from_timestamp=self.report.start.strftime(TIMESTAMP_FORMAT),
            to_timestamp=self.report.end.strftime(TIMESTAMP_FORMAT)
        )
        self.assertEqual(result, expected)

    def test_instantiate_sql_when_exploded_by_wiki(self):
        self.report.explode_by = {'wiki': 'wiki'}
        self.report.sql_template = 'SOME sql WITH "{wiki}";'
        sql_query = self.executor.instantiate_sql(self.report)
        self.assertEqual(sql_query, 'SOME sql WITH "wiki";')

    def test_create_connection_when_mysqldb_connect_raises_error(self):
        mysqldb_connect_stash = pymysql.connect
        pymysql.connect = MagicMock(side_effect=Exception())
        with self.assertRaises(RuntimeError):
            self.executor.create_connection(self.db_config, 'database')
        pymysql.connect = mysqldb_connect_stash

    def test_create_connection(self):
        mysqldb_connect_stash = pymysql.connect
        pymysql.connect = MagicMock(return_value='connection')
        connection = self.executor.create_connection(self.db_config, 'database')
        self.assertEqual(connection, 'connection')
        pymysql.connect = mysqldb_connect_stash

    def test_execute_sql_when_mysqldb_execution_raises_error(self):
        def execute_callback(sql_query):
            raise Exception()
        connection = ConnectionMock(execute_callback, None, [])
        with self.assertRaises(RuntimeError):
            self.executor.execute_sql('SOME sql;', connection)

    def test_execute_sql(self):
        def fetchall_callback():
            return [
                [date(2015, 1, 1), '1'],
                [date(2015, 1, 2), '2']
            ]
        connection = ConnectionMock(None, fetchall_callback, [])
        result = self.executor.execute_sql('SOME sql;', connection)
        expected = ([], [[date(2015, 1, 1), '1'], [date(2015, 1, 2), '2']])
        self.assertEqual(result, expected)

    def test_run_when_databases_is_not_in_config(self):
        del self.config['databases']
        with self.assertRaises(KeyError):
            list(self.executor.run())

    def test_run_when_helper_method_raises_error(self):
        selected = [self.report]
        self.executor.selector.run = MagicMock(return_value=selected)
        self.executor.instantiate_sql = MagicMock(side_effect=Exception())
        executed = list(self.executor.run())
        self.assertEqual(len(executed), 0)

    def test_execute_script_report_simple_params(self):
        self.report.explode_by = {}

        class PopenReturnMock():
            def __init__(self):
                self.stdout = []

        def subprocess_popen_mock(parameters, **kwargs):
            self.assertEqual(parameters[1], self.report.start.strftime(DATE_FORMAT))
            self.assertEqual(parameters[2], self.report.end.strftime(DATE_FORMAT))
            return PopenReturnMock()
        subprocess_popen_stash = subprocess.Popen
        subprocess.Popen = MagicMock(wraps=subprocess_popen_mock)
        self.executor.execute_script_report(self.report)
        subprocess.Popen = subprocess_popen_stash

    def test_execute_script_report_extra_params(self):
        self.report.explode_by = {'param1': 'value1', 'param2': 'value2'}

        class PopenReturnMock():
            def __init__(self):
                self.stdout = []

        def subprocess_popen_mock(parameters, **kwargs):
            self.assertEqual(parameters[3], 'value1')
            self.assertEqual(parameters[4], 'value2')
            return PopenReturnMock()
        subprocess_popen_stash = subprocess.Popen
        subprocess.Popen = MagicMock(wraps=subprocess_popen_mock)
        self.executor.execute_script_report(self.report)
        subprocess.Popen = subprocess_popen_stash

    def test_execute_script_when_script_raises_error(self):
        subprocess_popen_stash = subprocess.Popen
        subprocess.Popen = MagicMock(side_effect=OSError())
        success = self.executor.execute_script_report(self.report)
        subprocess.Popen = subprocess_popen_stash
        self.assertEqual(success, False)

    def test_execute_script(self):
        class PopenReturnMock():
            def __init__(self):
                self.stdout = ['date\tvalue', '2015-01-01\t1']

        def subprocess_popen_mock(parameters, **kwargs):
            return PopenReturnMock()
        subprocess_popen_stash = subprocess.Popen
        subprocess.Popen = MagicMock(wraps=subprocess_popen_mock)
        success = self.executor.execute_script_report(self.report)
        subprocess.Popen = subprocess_popen_stash
        self.assertEqual(success, True)
        self.assertEqual(self.report.results['header'], ['date', 'value'])
        expected_data = {datetime(2015, 1, 1): [datetime(2015, 1, 1), '1']}
        self.assertEqual(self.report.results['data'], expected_data)

    def test_normalize_results_when_header_is_not_set(self):
        data = [['date', 'col1', 'col2'], ['2016-01-01', 1, 2]]
        results = self.executor.normalize_results(self.report, None, data)
        expected = {
            'header': ['date', 'col1', 'col2'],
            'data': {
                datetime(2016, 1, 1): [datetime(2016, 1, 1), 1, 2]
            }
        }
        self.assertEqual(results, expected)

    def test_normalize_results_when_first_column_is_not_a_date(self):
        header = ['date', 'col1', 'col2']
        data = [
            [date(2015, 1, 1), 1, 2],
            ['bad formated date', 1, 2]
        ]
        with self.assertRaises(ValueError):
            self.executor.normalize_results(self.report, header, data)

    def test_normalize_results_when_data_is_empty(self):
        header = ['date', 'col1', 'col2']
        data = []
        results = self.executor.normalize_results(self.report, header, data)
        expected = {
            'header': ['date', 'col1', 'col2'],
            'data': {
                datetime(2015, 1, 1): [datetime(2015, 1, 1), None, None]
            }
        }
        self.assertEqual(results, expected)

    def test_normalize_results_with_funnel_data(self):
        header = ['date', 'val']
        data = [
            [date(2015, 1, 1), '1'],
            [date(2015, 1, 1), '2'],
            [date(2015, 1, 1), '3'],
            [date(2015, 1, 2), '4'],
            [date(2015, 1, 2), '5']
        ]
        self.report.is_funnel = True
        results = self.executor.normalize_results(self.report, header, data)
        expected = {
            'header': ['date', 'val'],
            'data': {
                datetime(2015, 1, 1): [
                    [datetime(2015, 1, 1), '1'],
                    [datetime(2015, 1, 1), '2'],
                    [datetime(2015, 1, 1), '3']
                ],
                datetime(2015, 1, 2): [
                    [datetime(2015, 1, 2), '4'],
                    [datetime(2015, 1, 2), '5']
                ]
            }
        }
        self.assertEqual(results, expected)

    def test_run(self):
        selected = [self.report]
        self.executor.selector.run = MagicMock(return_value=selected)
        self.executor.create_connection = MagicMock(return_value='connection')
        results = (
            ['some', 'sql', 'header'],
            [[date(2015, 1, 1), 'some', 'value']]
        )
        self.executor.execute_sql = MagicMock(return_value=results)
        executed = list(self.executor.run())
        self.assertEqual(len(executed), 1)
        report = executed[0]
        expected = {
            'header': ['some', 'sql', 'header'],
            'data': {datetime(2015, 1, 1): [datetime(2015, 1, 1), 'some', 'value']}
        }
        self.assertEqual(report.results, expected)
