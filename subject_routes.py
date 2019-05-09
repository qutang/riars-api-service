from app import app
import os
import pandas as pd
from model.subject import Subject, SubjectSchema
from flask import request, jsonify
import logging
import shutil

SUBJECT_META = os.path.join('data-logging', 'DerivedCrossParticipants',
                            'subjects.csv')


@app.route('/api/subjects', methods=['GET'])
def get_all_subjects():

    if os.path.exists(SUBJECT_META):
        subjects_df = pd.read_csv(SUBJECT_META)
        subjects = []
        subjects_json = subjects_df.to_dict('records')
        for subject_json in subjects_json:
            logging.debug(subject_json)
            new_subject = Subject.from_json_request(subject_json)
            if app.config['SELECTED_SUBJECT'] == subject_json['id']:
                new_subject.selected = True
            subjects.append(new_subject)
        result = Subject.to_json_responses(*subjects)
    else:
        result = []
    return jsonify(result)


@app.route('/api/subjects', methods=['PUT'])
def create_new_subject():
    body = request.json
    subject = Subject.from_json_request(body, many=False)
    logging.debug(body)
    logging.debug(subject)
    subject_df = pd.DataFrame(data=[body])
    # select subject
    app.config['SELECTED_SUBJECT'] = subject.id
    subject.selected = True
    # create subject data folder
    if os.path.exists(os.path.join('data-logging', subject.id)):
        logging.warning(subject.id + ' data folder exists, skip')
    else:
        os.makedirs(os.path.join('data-logging', subject.id))

    # write to subject meta file
    SUBJECT_META = os.path.join('data-logging', 'DerivedCrossParticipants',
                                'subjects.csv')
    if os.path.exists(SUBJECT_META):
        existing_subjects = pd.read_csv(SUBJECT_META)
        logging.debug(existing_subjects['id'])
        if subject.id in existing_subjects['id'].values:
            logging.warning(subject.id + ' is already in meta file')
        else:
            subject_df.to_csv(
                SUBJECT_META, index=False, header=False, mode='a')
    else:
        os.makedirs(os.path.dirname(SUBJECT_META), exist_ok=True)
        subject_df.to_csv(SUBJECT_META, index=False, header=True)
    result = subject.to_json_response()
    return jsonify(result), 200


@app.route('/api/subjects', methods=['POST'])
def select_subject():
    body = request.json
    logging.debug(body)
    subject = Subject.from_json_request(body)
    logging.debug(subject.id)
    # check if subject exists
    if os.path.exists(os.path.join('data-logging', subject.id)):
        app.config['SELECTED_SUBJECT'] = subject.id
        subject.selected = True
        result = subject.to_json_response()
        return jsonify(result), 200
    else:
        subject.selected = False
        result = subject.to_json_response()
        return jsonify(result), 405


@app.route('/api/subjects', methods=['DELETE'])
def remove_subject():
    body = request.json
    subject = Subject.from_json_request(body)
    # remove from current selected subject
    app.config['SELECTED_SUBJECT'] = None
    # remove from subject meta file
    subjects_df = pd.read_csv(SUBJECT_META)
    subjects_df = subjects_df[subjects_df['id'] != subject.id]
    subjects_df.to_csv(SUBJECT_META, index=False, header=True)
    # remove subject data folder
    shutil.rmtree(os.path.join('data-logging', subject.id))
    return jsonify('success'), 200