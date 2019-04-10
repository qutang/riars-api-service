from marshmallow import Schema, fields, post_load
import getip
import urllib


class Processor(object):
    def __init__(self,
                 name=None,
                 url=None,
                 host=None,
                 port=None,
                 connectable_url=None,
                 device_urls=[],
                 window_size=None,
                 update_rate=None,
                 error_code=None,
                 update_time=None,
                 status=None):
        self.name = name
        self.url = url
        self.host = host
        self.port = port
        self.connectable_url = connectable_url
        self.device_urls = device_urls
        self.window_size = window_size
        self.update_rate = update_rate
        self.error_code = error_code
        self.status = status
        self.update_time = update_time

    def validate_name(self, url_name):
        return url_name == self.name

    def get_connectable_host(self):
        if self.host == '0.0.0.0':
            return getip.get()

    def get_connectable_url(self):
        return 'ws://' + self.get_connectable_host() + ':' + str(self.port)

    def get_parsed_url(self, url):
        parsed = urllib.parse.urlparse(url)
        return parsed.hostname, parsed.port

    @staticmethod
    def from_json_request(body, many=False):
        schema = ProcessorSchema(many=many)
        processor = schema.load(body).data
        return processor

    @staticmethod
    def to_json_responses(*processors, timestamp=None):
        if timestamp is not None:
            for processor in processors:
                processor.update_time = timestamp
        schema = ProcessorSchema(many=True)
        return schema.dump(processors).data

    def to_json_response(self, timestamp=None):
        if timestamp is not None:
            self.update_time = timestamp
        schema = processorSchema()
        return schema.dump(self).data


class ProcessorSchema(Schema):
    name = fields.Str()
    url = fields.URL()
    host = fields.Str()
    port = fields.Integer()
    connectable_url = fields.Str()
    device_urls = fields.List(fields.Str())
    window_size = fields.Float()
    update_rate = fields.Float()
    error_code = fields.Str()
    status = fields.Str()
    update_time = fields.Float()

    @post_load
    def make_processor(self, data):
        return Processor(**data)
