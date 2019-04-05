from app import app, find_device_state_by_address, remove_device_state_by_address
import multiprocessing
import tasks
import ctypes
import logging
import time
from padar_realtime.metawear_stream import MetaWearStream, MetaWearScanner
from flask import request, jsonify
from model.device import Device


@app.route('/api/sensors', methods=['GET'])
def get_all():
    device_manager = app.config['DEVICE_MANAGER']
    devices = list(
        map(lambda device_state: device_state['device'], device_manager))
    scanner = MetaWearScanner()
    addresses = scanner.scan()
    devices += [
        Device(address=address, status='stopped') for address in addresses
    ]
    response = Device.to_json_responses(*devices, timestamp=time.time())
    return jsonify(response)


@app.route('/api/sensors/<address>', methods=['PUT'])
def run(address):
    m = app.config['PROCESS_MANAGER']
    device_manager = app.config['DEVICE_MANAGER']
    body = request.json

    device = Device.from_json_request(body)
    if device.validate_address(address):
        found = find_device_state_by_address(address, device_manager)
        if found is None:
            state_holder = m.Value(ctypes.c_char_p, 'stopped')
            stop_signal = multiprocessing.Event()
            logging.info('Start a sensor: ' + address)
            p = multiprocessing.Process(
                target=tasks.run_sensor,
                args=(state_holder, stop_signal, device))
            p.start()
            time.sleep(2)
            # prepare response json
            device.status = state_holder.value
            device_manager.append({
                'stop_signal': stop_signal,
                'state': state_holder,
                'process': p,
                'device': device
            })
        elif found['device'].status == 'running':
            error_code = 'Device is already running'
            logging.info(error_code)
            logging.error(error_code)
            device.error_code = error_code
        else:
            remove_device_state_by_address(address, device_manager)
            state_holder = m.Value(ctypes.c_char_p, 'stopped')
            stop_signal = multiprocessing.Event()
            logging.info('Start a sensor: ' + address)
            p = multiprocessing.Process(
                target=tasks.run_sensor,
                args=(state_holder, stop_signal, device))
            p.start()
            time.sleep(2)
            # prepare response json
            device.status = state_holder.value
            device_manager.append({
                'stop_signal': stop_signal,
                'state': state_holder,
                'process': p,
                'device': device
            })
    else:
        error_code = 'Address in the url and address in the request body do not match'
        logging.error(error_code)
        device.error_code = error_code
        device.status = 'stopped'
    response = device.to_json_response(time.time())
    return jsonify(response)


@app.route('/api/sensors/<address>', methods=['GET'])
def get(address):
    device_manager = app.config['DEVICE_MANAGER']
    device_state = find_device_state_by_address(address, device_manager)
    if device_state is not None:
        response = device_state['device'].to_json_response(time.time())
    else:
        response = Device(
            address=address,
            error_code='Device is not found').to_json_response(time.time())
    return jsonify(response)


@app.route('/api/sensors/<address>', methods=['DELETE'])
def stop(address):
    device_manager = app.config['DEVICE_MANAGER']
    device_state = find_device_state_by_address(address, device_manager)
    if device_state is not None:
        stop_signal = device_state['stop_signal']
        state = device_state['state']
        p = device_state['process']
        device = device_state['device']
        stop_signal.set()
        while state.value != 'stopped':
            pass
        device.status = state.value
        remove_device_state_by_address(address, device_manager)
        logging.info('Stop a sensor: ' + address)
        response = device.to_json_response(time.time())
    else:
        device = Device(address=address, error_code='Device not found')
        logging.info('Sensor is not found')
        response = device.to_json_response(time.time())
    return jsonify(response)