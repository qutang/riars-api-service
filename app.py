import flask
import multiprocessing

app = flask.Flask(__name__)


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