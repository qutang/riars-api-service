from app import app
import os
import pandas as pd
from model.annotation import Annotation, AnnotationSchema
from flask import request, jsonify
import logging


@app.route('/api/annotations', methods=['PUT'])
def save_annotations():
    body = request.json
    annotations = Annotation.from_json_request(body, many=True)
    annotation_df = Annotation.to_dataframe(annotations)
    annotation_df['START_TIME'] = pd.to_datetime(
        annotation_df['START_TIME'], unit='s')
    annotation_df['HEADER_TIME_STAMP'] = pd.to_datetime(
        annotation_df['HEADER_TIME_STAMP'], unit='s')
    annotation_df['STOP_TIME'] = pd.to_datetime(
        annotation_df['STOP_TIME'], unit='s')
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
                               annotations[0].id + '.annotation.csv')
    annotation_df.to_csv(output_file, index=False)
    return "success", 200
