
# Records results to Graphite

import time
import socket
from utils import raise_critical


class Graphite(object):
    timestamp = None

    def __init__(self, config, timestamp=None):
        """
        At the top level, graphite configuration is expected to look like:

        graphite:
            host: domain.tld
            port: 123
            lookups:
                wiki: sitematrix.yaml
                      (file name is replaced by the loaded config on startup)

        Here, lookups is a dictionary of lookup yaml files.  These files are
        loaded into python dictionaries by reportupdater on startup, and used
        by graphite to translate things like wiki db names into something
        friendlier.
        """
        graphite = config['graphite']
        if not isinstance(graphite, dict):
            raise TypeError('Graphite must be a dict')
        if not isinstance(graphite.get('host'), str):
            raise TypeError('Graphite host must be a string')
        if not isinstance(graphite.get('port'), int):
            raise TypeError('Graphite port must be an int')
        self.host = graphite['host']
        self.port = graphite['port']
        self.lookups = graphite.get('lookups', {})

        # Time really needs to be preserved
        self.timestamp = timestamp or int(time.time())

    def record_row(self, row, report):
        """
        At the report level, graphite configuration is expected to look like:

        graphite:
            path: '{_metric}.{wiki}'
            metrics:
                - metric-name-one: column-name-in-report
                - metric-name-two: column-name-in-report

        Here the path is a python string template that should be possible to fill
        in from the report.explode_by config.  If the top level graphite config has
        defined lookups, those will be replaced accordingly in explode_by.  The
        special key _metric in path will be replaced with the metric name from the
        metrics list.
        """
        if len(report.graphite) == 0:
            return

        for metric in report.graphite['metrics']:
            (metric, value, timestamp) = self.get_graphite_data(report, row, metric)
            self.record(metric, value, timestamp)

    def get_graphite_data(self, report, row, metric):
        header = report.results['header']
        # add all the values the report has been exploded by
        data = report.explode_by
        # swap out any values that have lookups in the main graphite config
        # TODO: instead of using local files, work on a plugin system for reportupdater
        for key, value in data.items():
            if key in self.lookups:
                data[key] = self.lookups[key].get(value, value)
        # add values from this row labeled by the header
        for col, label in enumerate(header):
            data[label] = row[col]
        data['_metric'] = metric

        try:
            graphite_metric = report.graphite['path'].format(**data)
        except:
            raise_critical(ValueError, 'Invalid format "{}" with {}'.format(report.graphite['path'], data))

        try:
            column_name = report.graphite['metrics'][metric]
            column_index = header.index(column_name)
            value = row[column_index]
        except:
            raise_critical(ValueError, 'Could not find {} in {} with header {}'.format(metric, row, header))

        timestamp = time.mktime(row[0].timetuple())

        return graphite_metric, value, timestamp

    def record(self, metric, value, timestamp=None):
        if ' ' in metric or '"' in metric:
            raise_critical(ValueError, 'Invalid metric name "{}"'.format(metric))

        timestamp = int(timestamp or self.timestamp)

        sock = socket.socket()
        sock.connect((self.host, int(self.port)))
        sock.send('{} {} {}\n'.format(metric, value, timestamp))
        sock.close()
