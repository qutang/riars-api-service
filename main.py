from app import app
import device_routes
import processor_routes
import subject_routes
import logging
import multiprocessing
import multiprocessing_logging
import os
from model.processor import Processor


def set_app_states(debug=True, logging_level=logging.DEBUG):
    if debug:
        multiprocessing.log_to_stderr(logging_level)

    logging.basicConfig(
        level=logging_level,
        format=
        '[%(levelname)s]{%(processName)s-%(process)d}(%(threadName)s-%(thread)d)%(message)s',
    )

    multiprocessing_logging.install_mp_handler()

    app.config['PROCESS_MANAGER'] = multiprocessing.Manager()
    app.config['DEVICE_MANAGER'] = []
    app.config['PROCESSOR_MANAGER'] = []
    app.config['SELECTED_SUBJECT'] = None


def initialize_processors():
    candidates = os.listdir('processors')
    processor_names = list(
        map(
            lambda c: c.replace('.py', ''),
            filter(lambda c: 'model' not in c and 'pycache' not in c,
                   candidates)))
    logging.info('Found processors: ' + ','.join(processor_names))
    for name in processor_names:
        app.config['PROCESSOR_MANAGER'].append({
            'processor':
            Processor(name=name, status='stopped'),
            'stop_signal':
            None,
            'state':
            None,
            'process':
            None
        })


if __name__ == '__main__':
    debug = True
    logging_level = logging.DEBUG
    set_app_states(debug=debug, logging_level=logging_level)
    initialize_processors()
    app.run(host='0.0.0.0', port=5000, debug=debug)
