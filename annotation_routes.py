from app import app
import os
import pandas as pd
from model.annotation import Annotation, AnnotationSchema
from flask import request, jsonify
import logging


@app.route('/api/annotations', methods=['PUT'])
def save_annotations():
    body = request.json
    annotations = Annotation.from_json_request(body['data'], many=True)

    id = body['id']
    data_type = body['type']
    annotation_df = Annotation.to_dataframe(annotations)
    annotation_df['START_TIME'] = annotation_df['START_TIME'].apply(
        pd.Timestamp.fromtimestamp)
    annotation_df['HEADER_TIME_STAMP'] = annotation_df[
        'HEADER_TIME_STAMP'].apply(pd.Timestamp.fromtimestamp)
    annotation_df['STOP_TIME'] = annotation_df['STOP_TIME'].apply(
        pd.Timestamp.fromtimestamp)
    timestamp = annotation_df['START_TIME'][0]
    timestamp = timestamp.strftime('%Y-%m-%d-%H-%M-%S-%f')
    timestamp = timestamp[:-3] + "-P0000"
    # save annotations to file
    if app.config['SELECTED_SUBJECT'] is None:
        logging_folder = False
    else:
        logging_folder = os.path.abspath(
            os.path.join('data-logging', app.config['SELECTED_SUBJECT'],
                         'MasterSynced'))
    os.makedirs(logging_folder, exist_ok=True)
    logging.info('Create logging folder: ' + logging_folder)
    output_file = os.path.join(logging_folder,
                               id + '.' + timestamp + '.' + data_type + '.csv')
    annotation_df.to_csv(output_file, index=False)
    return "success", 200
