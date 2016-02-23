#!/usr/bin/python

import logging
import argparse
from reportupdater import reportupdater


LOGGING_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


def main():
    parser = argparse.ArgumentParser(
        description=('Periodically execute SQL queries or scripts ' +
                     'and write/update the results into TSV files.'))
    parser.add_argument('query_folder',
                        help='Folder with *.sql files and scripts.')
    parser.add_argument('output_folder',
                        help='Folder to write the TSV files to.')
    parser.add_argument('--config-path',
                        help='Yaml configuration file. Default: <query_folder>/config.yaml.')
    parser.add_argument('--wikis_path',
                        help='All wikis list file path. Default: wikis.txt.')
    parser.add_argument('-l', '--log-level',
                        help='(debug|info|warning|error|critical)')
    args = vars(parser.parse_args())
    if 'log_level' in args:
        if args['log_level'] in LOGGING_LEVELS:
            args['log_level'] = LOGGING_LEVELS[args['log_level']]
        else:
            del args['log_level']
    reportupdater.run(**args)


if __name__ == '__main__':
    main()
