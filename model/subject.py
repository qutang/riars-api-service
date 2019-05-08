from marshmallow import Schema, fields, post_load


class Subject(object):
    def __init__(self,
                 id=None,
                 age=None,
                 gender=None,
                 weight=None,
                 height=None,
                 dress=None,
                 shoes=None,
                 dominant_side=None,
                 selected=False):
        self.id = id
        self.gender = gender
        self.age = age
        self.weight = weight
        self.height = height
        self.dress = dress
        self.shoes = shoes
        self.dominant_side = dominant_side
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
    weight = fields.Float()
    height = fields.Float()
    dress = fields.Str()
    shoes = fields.Str()
    dominant_side = fields.Str()
    selected = fields.Bool()

    @post_load
    def build_subject(self, data):
        return Subject(**data)
