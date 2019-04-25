import flask
import multiprocessing
from flask_cors import CORS

app = flask.Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})


def find_device_state_by_address(address, device_manager):
    for device_state in device_manager:
        device = device_state['device']
        if device.validate_address(address):
            return device_state
    return None


def remove_device_state_by_address(address, device_manager):
    found = None
    for device_state in device_manager:
        device = device_state['device']
        if device.validate_address(address):
            found = device_state
            break
    device_manager.remove(device_state)


def find_processor_state_by_name(name, processor_manager):
    for processor_state in processor_manager:
        processor = processor_state['processor']
        if processor.validate_name(name):
            return processor_state
    return None


def remove_processor_state_by_name(name, processor_manager):
    found = None
    for processor_state in processor_manager:
        processor = processor_state['processor']
        if processor.validate_name(name):
            found = processor_state
            break
    processor_manager.remove(processor_state)
