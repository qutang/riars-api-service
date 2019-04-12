from marshmallow import Schema, fields, post_load


class Subject(object):
    def __init__(self, id=None, age=None, gender=None, selected=False):
        self.id = id
        self.gender = gender
        self.age = age
        self.selected = selected

    @staticmethod
    def from_json_request(body, many=False):
        schema = SubjectSchema(many=many)
        subject = schema.load(body).data
        return subject

    @staticmethod
    def to_json_responses(*subjects):
        schema = SubjectSchema(many=True)
        return schema.dump(subjects).data

    def to_json_response(self):
        schema = SubjectSchema()
        return schema.dump(self).data


class SubjectSchema(Schema):
    id = fields.Str()
    gender = fields.Str()
    age = fields.Integer()
    selected = fields.Bool()

    @post_load
    def build_subject(self, data):
        return Subject(**data)
