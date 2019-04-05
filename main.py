from app import app
import device_routes
import logging
import multiprocessing
import multiprocessing_logging


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


if __name__ == '__main__':
    debug = True
    logging_level = logging.DEBUG
    set_app_states(debug=debug, logging_level=logging_level)
    app.run(host='0.0.0.0', port=5000, debug=debug)
