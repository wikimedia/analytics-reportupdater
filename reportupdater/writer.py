
# This module is the last step of the pipeline.
# It gets the results passed from the executor,
# and updates the report's corresponding file.


import os
import io
import csv
import logging
from copy import copy, deepcopy
from executor import Executor
from utils import (raise_critical, get_previous_results,
                   DATE_FORMAT, get_exploded_report_output_path,
                   get_increment)


class Writer(object):


    def __init__(self, executor, config, graphite=None):
        if not isinstance(executor, Executor):
            raise_critical(ValueError, 'Executor is not valid.')
        if not isinstance(config, dict):
            raise_critical(ValueError, 'Config is not a dict.')
        self.executor = executor
        self.config = config
        self.graphite = graphite


    def get_output_folder(self):
        if 'output_folder' not in self.config:
            raise KeyError('Output folder is not in config.')
        output_folder = self.config['output_folder']
        if not isinstance(output_folder, str):
            raise ValueError('Output folder is not a string.')

        return output_folder


    def run(self):
        for report in self.executor.run():
            logging.debug('Writing "{report}"...'.format(report=str(report)))

            header, updated_data, new_dates = self.update_results(report)
            try:
                self.write_results(header, updated_data, report, self.get_output_folder())
                self.record_to_graphite(report, new_dates)
                logging.info('Report {report_key} has been updated.'.format(report_key=report.key))
            except Exception, e:
                message = ('Report "{report_key}" could not be written '
                           'because of error: {error}')
                logging.error(message.format(report_key=report.key, error=str(e)))


    def update_results(self, report):
        """
        Returns
            (header, updated_data, new_dates)
            header          : the new header with any changes inferred from results
            updated_data    : the new data, including reruns
            new_dates       : list of only the new dates output, excluding rerun dates
        """
        # Get current results.
        current_header = copy(report.results['header'])
        current_data = deepcopy(report.results['data'])
        for date in current_data:
            rows = current_data[date] if report.is_funnel else [current_data[date]]
            for row in rows:
                if len(row) != len(current_header):
                    raise ValueError('Results and header do not match.')

        # Get previous results (no need to pass the reruns, they will be overwritten).
        previous_results = get_previous_results(report, self.get_output_folder(), {})
        previous_header = previous_results['header']
        previous_data = previous_results['data']
        if not previous_header:
            if not previous_data:
                previous_header = current_header
            else:
                raise ValueError('Previous results have no header.')

        # The new dates will not include rerun dates, as those should always be in previous data
        new_dates = list(set(current_data.keys()) - set(previous_data.keys()))

        # Current results may have a different header than previous results.
        # They may contain new columns, column order changes, or removal
        # of some columns.
        if current_header != previous_header:

            # Rewrite current header and data to include removed columns.
            removed_columns = sorted(list(set(previous_header) - set(current_header)))
            if removed_columns:
                current_header.extend(removed_columns)
                for date in current_data:
                    rows = current_data[date] if report.is_funnel else [current_data[date]]
                    for row in rows:
                        row.extend([None] * len(removed_columns))

            # Make a map to use when updating previous data column order.
            column_map = [
                (current_header.index(col), previous_header.index(col))
                for col in set(current_header).intersection(set(previous_header))
            ]

            # Rewrite previous data in the new order and including new columns.
            for date in previous_data:
                rows = previous_data[date] if report.is_funnel else [previous_data[date]]
                rewritten_rows = []
                for row in rows:
                    rewritten_row = [None] * len(current_header)
                    for new_index, old_index in column_map:
                        rewritten_row[new_index] = row[old_index]
                    rewritten_rows.append(rewritten_row)
                previous_data[date] = rewritten_rows if report.is_funnel else rewritten_rows[0]

        # Build final updated data.
        updated_header = current_header
        updated_data = {}
        date_threshold = self.get_date_threshold(report, previous_data)
        for date in previous_data:
            if not date_threshold or date > date_threshold:
                updated_data[date] = previous_data[date]
        updated_data.update(current_data)

        return updated_header, updated_data, new_dates


    def write_results(self, header, data, report, output_folder):
        dates = sorted(data.keys())
        rows = [data[date] for date in dates]
        if report.is_funnel:
            rows = [row for sublist in rows for row in sublist]  # flatten
        if len(report.explode_by) > 0:
            output_path = get_exploded_report_output_path(
                output_folder, report.explode_by, report.key)
        else:
            output_path = os.path.join(output_folder, report.key + '.tsv')
        temp_output_path = output_path + '.tmp'

        # Make sure the output directory exists
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        try:
            # wb mode needed to avoid unicode conflict between io and csv
            temp_output_file = io.open(temp_output_path, 'wb')
        except Exception, e:
            raise RuntimeError('Could not open the temporary output file (' + str(e) + ').')
        tsv_writer = csv.writer(temp_output_file, delimiter='\t')
        tsv_writer.writerow(header)
        for row in rows:
            row[0] = row[0].strftime(DATE_FORMAT)
            tsv_writer.writerow(row)
        temp_output_file.close()
        try:
            os.rename(temp_output_path, output_path)
        except Exception, e:
            raise RuntimeError('Could not rename the output file (' + str(e) + ').')


    def get_date_threshold(self, report, previous_data):
        if not report.max_data_points:
            return None
        # Note that some older python-dateutil versions have
        # problems when multiplying relativedelta instances.
        increment = get_increment(report.granularity, report.max_data_points)
        last_data_point = max(previous_data.keys() + [report.start])
        return last_data_point - increment

    def record_to_graphite(self, report, dates_to_send):
        if self.graphite is None:
            return

        data = report.results['data']
        dates = sorted(dates_to_send)
        rows = [data[date] for date in dates]
        if report.is_funnel:
            rows = [row for sublist in rows for row in sublist]  # flatten
        for row in rows:
            self.graphite.record_row(row, report)
