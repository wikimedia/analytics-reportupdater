
# This module is the last step of the pipeline.
# It gets the results passed from the executor,
# and updates the report's corresponding file.
#
# In the case of timeboxed reports, it handles the
# update of previous results consistently.


import os
import io
import csv
import logging
from executor import Executor
from utils import (raise_critical, get_previous_results,
                   DATE_FORMAT, get_exploded_report_output_path)


class Writer(object):


    def __init__(self, executor, config):
        if not isinstance(executor, Executor):
            raise_critical(ValueError, 'Executor is not valid.')
        if not isinstance(config, dict):
            raise_critical(ValueError, 'Config is not a dict.')
        self.executor = executor
        self.config = config


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

            header, updated_data = self.update_results(report)

            try:
                self.write_results(header, updated_data, report, self.get_output_folder())
                logging.info('Report {report_key} has been updated.'.format(report_key=report.key))
            except Exception, e:
                message = ('Report "{report_key}" could not be written '
                           'because of error: {error}')
                logging.error(message.format(report_key=report.key, error=str(e)))


    def update_results(self, report):
        header = report.results['header']
        previous_results = get_previous_results(report, self.get_output_folder())
        previous_header = previous_results['header']

        updated_data = {}

        # Handle the first run case
        if not previous_header:
            if not previous_results['data']:
                previous_header = header
            else:
                raise ValueError('Previous results have no header')

        # NOTE: this supports moving columns and adding columns
        #       it will rewrite the old data accordingly
        if header != previous_header:
            old_columns = set(header).intersection(set(previous_header))
            removed_columns = list(set(previous_header) - set(header))

            # removed columns are not supported yet
            if removed_columns:
                raise ValueError('Results header is missing ' + str(removed_columns))

            # make a map to use when updating old rows to new rows
            new_indexes = {
                header.index(col): previous_header.index(col)
                for col in old_columns
            }

            # rewrite previous results if there are new columns
            for date, rows in previous_results['data'].items():
                rows_with_nulls = []
                iteratee = rows
                if not report.is_funnel:
                    iteratee = [rows]
                for row in iteratee:
                    updated_row = [None] * len(header)
                    for new_index, old_index in new_indexes.items():
                        updated_row[new_index] = row[old_index]

                    if report.is_funnel:
                        rows_with_nulls.append(updated_row)
                    else:
                        rows_with_nulls = updated_row

                updated_data[date] = rows_with_nulls
        else:
            updated_data = previous_results['data']

        for date, rows in report.results['data'].iteritems():
            updated_data[date] = rows
            if report.is_funnel:
                for row in rows:
                    if len(row) != len(header):
                        raise ValueError('Results and Header do not match')
            else:
                if len(rows) != len(header):
                    raise ValueError('Results and Header do not match')

        return header, updated_data


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
