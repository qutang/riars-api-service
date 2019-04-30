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
    return jsonify(response), 200


@app.route('/api/sensors', methods=['PUT'])
def run_all():
    process_manager = app.config['PROCESS_MANAGER']
    device_manager = app.config['DEVICE_MANAGER']
    body = request.json

    devices = Device.from_json_request(body, many=True)
    results = []
    for device in devices:
        address = device.address
        device = _run(address, device, device_manager, process_manager)
        results.append(device)
    response = Device.to_json_responses(*results, time.time())
    return jsonify(response), 200


@app.route('/api/sensors', methods=['DELETE'])
def stop_all():
    device_manager = app.config['DEVICE_MANAGER']
    print(request.json)
    devices = Device.from_json_request(request.json, many=True)
    addresses = list(map(lambda d: d.address, devices))
    results = []
    for address in addresses:
        device = _stop(address, device_manager)
        results.append(device)
    response = Device.to_json_responses(*results, time.time())
    return jsonify(response)


@app.route('/api/sensors/<address>', methods=['PUT'])
def run(address):
    process_manager = app.config['PROCESS_MANAGER']
    device_manager = app.config['DEVICE_MANAGER']
    body = request.json
    device = Device.from_json_request(body)
    device = _run(address, device, device_manager, process_manager)
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
    device = _stop(address, device_manager)
    response = device.to_json_response(time.time())
    return jsonify(response)


def _run(address, device, device_manager, process_manager):
    if device.validate_address(address):
        found = find_device_state_by_address(address, device_manager)
        if found is None:
            state_holder = process_manager.Value(ctypes.c_char_p, 'stopped')
            stop_signal = multiprocessing.Event()
            logging.info('Start a sensor: ' + address)
            p = multiprocessing.Process(
                target=tasks.run_sensor,
                args=(state_holder, stop_signal, device))
            p.daemon = True
            p.start()
            time.sleep(2)
            # prepare response json
            device.connectable_url = device.get_ws_url()
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
            device.status = 'running'
        else:
            remove_device_state_by_address(address, device_manager)
            state_holder = process_manager.Value(ctypes.c_char_p, 'stopped')
            stop_signal = multiprocessing.Event()
            logging.info('Start a sensor: ' + address)
            p = multiprocessing.Process(
                target=tasks.run_sensor,
                args=(state_holder, stop_signal, device))
            p.start()
            while state_holder.value != 'running':
                p.join(2)
                if not p.is_alive():
                    break
            # prepare response json
            device.connectable_url = device.get_ws_url()
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
    return device


def _stop(address, device_manager):
    device_state = find_device_state_by_address(address, device_manager)
    if device_state is not None:
        stop_signal = device_state['stop_signal']
        state = device_state['state']
        p = device_state['process']
        device = device_state['device']
        stop_signal.set()
        while state.value != 'stopped':
            pass
        p.terminate()
        device.status = state.value
        remove_device_state_by_address(address, device_manager)
        logging.info('Stop a sensor: ' + address)
    else:
        device = Device(address=address, error_code='Device not found')
        logging.info('Sensor is not found')
    return device