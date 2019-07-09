from marshmallow import Schema, fields, post_load
import pandas as pd


class Annotation(object):
    def __init__(self,
                 start_time=None,
                 stop_time=None,
                 label_name=None,
                 category=None,
                 note=None):
        self.start_time = start_time
        self.stop_time = stop_time
        self.label_name = label_name
        self.category = category
        self.note = note

    @staticmethod
    def from_json_request(body, many=False):
        print(body)
        schema = AnnotationSchema(many=many)
        annotation = schema.load(body).data
        return annotation

    @staticmethod
    def to_json_responses(*annotations):
        schema = AnnotationSchema(many=True)
        return schema.dump(annotations).data

    def to_json_response(self):
        schema = AnnotationSchema()
        return schema.dump(self).data

    def to_df(self):

        return pd.DataFrame(
            data={
                'HEADER_TIME_STAMP': [self.start_time],
                'START_TIME': [self.start_time],
                'STOP_TIME': [self.stop_time],
                'LABEL_NAME': [self.label_name],
                'NOTE': [str(self.category) + '_' + str(self.note)]
            })

    @staticmethod
    def to_dataframe(annotations):
        annotation_dfs = list(
            map(lambda annotation: annotation.to_df(), annotations))
        annotation_df = pd.concat(annotation_dfs, ignore_index=True)
        return annotation_df


class AnnotationSchema(Schema):
    start_time = fields.Float()
    stop_time = fields.Float()
    label_name = fields.Str()
    category = fields.Str()
    note = fields.Str()

    @post_load
    def build_annotation(self, data):
        return Annotation(**data)
