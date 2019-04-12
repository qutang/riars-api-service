from app import app, find_processor_state_by_name, remove_processor_state_by_name
import multiprocessing
import tasks
import ctypes
import logging
import time
from padar_realtime.processor_stream import ProcessorStreamManager
from flask import request, jsonify
from model.processor import Processor
import os
from threading import Thread


@app.route('/api/processors', methods=['GET'])
def get_all_p():
    processor_manager = app.config['PROCESSOR_MANAGER']
    processors = list(
        map(lambda processor_state: processor_state['processor'],
            processor_manager))
    response = Processor.to_json_responses(*processors, timestamp=time.time())
    return jsonify(response)


@app.route('/api/processors', methods=['PUT'])
def run_all_p():
    process_manager = app.config['PROCESS_MANAGER']
    processor_manager = app.config['PROCESSOR_MANAGER']
    body = request.json

    processors = Processor.from_json_request(body, many=True)
    results = []
    for processor in processors:
        name = processor.name
        processor = _run_p(name, processor, processor_manager, process_manager)
        results.append(processor)
    response = Processor.to_json_responses(*results, time.time())
    return jsonify(response)


@app.route('/api/processors', methods=['DELETE'])
def stop_all_p():
    processor_manager = app.config['PROCESSOR_MANAGER']
    processors = Processor.from_json_request(request.json, many=True)
    names = list(map(lambda psor: psor.name, processors))
    results = []
    for name in names:
        processor = _stop_p(name, processor_manager)
        results.append(processor)
    response = Processor.to_json_responses(*results, time.time())
    return jsonify(response)


@app.route('/api/processors/<name>', methods=['PUT'])
def run_p(name):
    process_manager = app.config['PROCESS_MANAGER']
    processor_manager = app.config['PROCESSOR_MANAGER']
    body = request.json
    processor = Processor.from_json_request(body)
    processor = _run_p(name, processor, processor_manager, process_manager)
    response = processor.to_json_response(time.time())
    return jsonify(response)


@app.route('/api/processors/<address>', methods=['GET'])
def get_p(name):
    processor_manager = app.config['PROCESSOR_MANAGER']
    processor_state = find_processor_state_by_name(name, processor_manager)
    if processor_state is not None:
        response = processor_state['processor'].to_json_response(time.time())
    else:
        response = Processor(
            name=name,
            error_code='Processor is not found').to_json_response(time.time())
    return jsonify(response)


@app.route('/api/processors/<name>', methods=['DELETE'])
def stop_p(name):
    processor_manager = app.config['PROCESSOR_MANAGER']
    processor = _stop_p(name, processor_manager)
    response = processor.to_json_response(time.time())
    return jsonify(response)


def _run_p(name, processor, processor_manager, process_manager):
    def _process_stop_monitor():
        logging.info('Start monitoring processor status')
        while state_holder.value == 'running':
            time.sleep(1)
        processor_state = find_processor_state_by_name(name, processor_manager)
        processor_state['processor'].status = state_holder.value
        processor_state['stop_signal'] = None
        processor_state['state'] = state_holder
        processor_state['process'] = None

    if processor.validate_name(name):
        found = find_processor_state_by_name(name, processor_manager)
        if found is None:
            error_code = 'Processor is not found'
            logging.info(error_code)
            logging.error(error_code)
            processor.error_code = error_code
        elif found['processor'].status == 'running':
            error_code = 'Processor is already running'
            logging.info(error_code)
            logging.error(error_code)
            processor.error_code = error_code
        else:
            remove_processor_state_by_name(name, processor_manager)
            state_holder = process_manager.Value(ctypes.c_char_p, 'stopped')
            stop_signal = multiprocessing.Event()
            if app.config['SELECTED_SUBJECT'] is None:
                logging_folder = False
            else:
                logging_folder = os.path.abspath(
                    os.path.join('data-logging',
                                 app.config['SELECTED_SUBJECT'],
                                 'MasterSynced'))
                os.makedirs(logging_folder, exist_ok=True)
                logging.info('Create logging folder: ' + logging_folder)
            logging.info('Start a processor: ' + name)
            p = multiprocessing.Process(
                target=tasks.run_processor,
                args=(state_holder, stop_signal, processor, logging_folder))
            p.start()
            while state_holder.value != 'running':
                p.join(2)
                if not p.is_alive():
                    break
            # start monitor
            Thread(target=_process_stop_monitor).start()
            # prepare response json
            processor.connectable_url = processor.get_connectable_url()
            processor.status = state_holder.value
            processor_manager.append({
                'stop_signal': stop_signal,
                'state': state_holder,
                'process': p,
                'processor': processor
            })
    else:
        error_code = 'Name in the url and name in the request body do not match'
        logging.error(error_code)
        processor.error_code = error_code
        processor.status = 'stopped'
    return processor


def _stop_p(name, processor_manager):
    processor_state = find_processor_state_by_name(name, processor_manager)
    if processor_state is not None and processor_state[
            'processor'].status == 'running':
        stop_signal = processor_state['stop_signal']
        state = processor_state['state']
        p = processor_state['process']
        processor = processor_state['processor']
        stop_signal.set()
        while state.value != 'stopped':
            pass
        processor.status = state.value
        logging.info('Stop a processor: ' + name)
    elif processor_state is not None and processor_state[
            'processor'].status == 'stopped':
        processor = Processor(
            name=name, error_code='Processor was already stopped')
        logging.info('Processor was already stopped')
    else:
        processor = Processor(name=name, error_code='Processor not found')
        logging.info('Processor is not found')
    return processor