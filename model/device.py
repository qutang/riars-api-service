from marshmallow import Schema, fields, post_load
import getip
import pandas as pd


class Device(object):
    def __init__(self,
                 name=None,
                 address=None,
                 sr=None,
                 grange=None,
                 status=None,
                 order=None,
                 timestamp=None,
                 url=None,
                 host=None,
                 port=None,
                 connectable_url=None,
                 error_code=None):
        self.name = name
        self.address = address
        self.sr = sr
        self.grange = grange
        self.status = status
        self.order = order
        self.host = host
        self.port = port
        self.update_time = timestamp
        self.url = url
        self.error_code = error_code
        self._connectable_url = connectable_url

    def validate_address(self, url_address):
        address = Device.recover_address(url_address)
        return address == self.address

    def get_connectable_host(self):
        if self.host == '0.0.0.0':
            return getip.get()
        else:
            return self.host

    def get_ws_url(self):
        return 'ws://' + self.get_connectable_host() + ':' + str(self.port)

    @staticmethod
    def strip_address(address):
        return address.replace(':', '-')

    @staticmethod
    def recover_address(address):
        return address.replace('-', ':')

    @staticmethod
    def from_json_request(body, many=False):
        schema = DeviceSchema(many=many)
        device = schema.load(body).data
        return device

    @staticmethod
    def to_json_responses(*devices, timestamp=None):
        if timestamp is not None:
            for device in devices:
                device.update_time = timestamp
        schema = DeviceSchema(many=True)
        return schema.dump(devices).data

    @staticmethod
    def to_location_mapping_csv(*devices, pid):
        records = map(lambda device: {
            "PID": pid,
            "SENSOR_ID": device.address,
            "SENSOR_PLACEMENT": device.name
        }, devices)
        return pd.DataFrame.from_records(data=records)

    def to_json_response(self, timestamp=None):
        if timestamp is not None:
            self.update_time = timestamp
        schema = DeviceSchema()
        return schema.dump(self).data


class DeviceSchema(Schema):
    name = fields.Str()
    address = fields.Str()
    sr = fields.Integer()
    grange = fields.Integer()
    status = fields.Str()
    order = fields.Integer()
    update_time = fields.Float()
    url = fields.URL()
    host = fields.Str()
    port = fields.Integer()
    error_code = fields.Str()
    connectable_url = fields.Str()

    @post_load
    def build_sensor(self, data):
        return Device(**data)
