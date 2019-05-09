from padar_features.feature_set import FeatureSet
import pandas as pd
import signal
import asyncio
from padar_realtime.processor_stream import ProcessorStreamManager
from functools import reduce
import os
import json
import pickle
import numpy as np
import logging

dir_path = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'pretrained_models')
PREDEFINED_MODELS = [
    os.path.join(dir_path, f) for f in [
        'DW.MO.thirteen_activities_model.pkl',
    ]
]


def entry(merged,
          model_files=None,
          logging_folder=False,
          number_of_windows=None):
    if model_files is None:
        model_files = PREDEFINED_MODELS
    os.makedirs('outputs', exist_ok=True)
    data_type = merged['DATA_TYPE']
    st = merged['MERGED_CHUNK_ST']
    et = merged['MERGED_CHUNK_ET']
    count = merged['MERGED_CHUNK_INDEX']
    stream_names = merged['STREAM_NAMES']
    chunks = merged['CHUNKS']
    if data_type != 'accel':
        return None
    else:
        feature_json, feature_df = _compute_features(chunks)
        prediction_json, prediction_df = _make_predictions(
            feature_df, model_files)
        if logging_folder == False or (count >= number_of_windows
                                       and number_of_windows > 0):
            logging.info('skip data logging')
        else:
            _save_raw_data(chunks, logging_folder)
            _save_features(feature_df, stream_names, logging_folder)
            _save_predictions(prediction_df, stream_names, logging_folder)
        result_json = {**feature_json, **prediction_json}
        result_json['START_TIME'] = st
        result_json['STOP_TIME'] = et
        result_json['INDEX'] = count
        json_str = json.dumps(result_json)
        logging.debug(json_str)
        return json_str


def _save_raw_data(chunks, logging_folder):
    for chunk in chunks:
        stream_name = chunk.get_stream_name()
        packages = chunk.get_packages()
        chunk_index = chunk.get_chunk_index()
        dfs = [package.to_dataframe() for package in packages]
        data_df = pd.concat(dfs, axis=0, ignore_index=True)
        data_df['CHUNK_INDEX'] = chunk_index
        data_df['HEADER_TIME_STAMP'] = data_df['HEADER_TIME_STAMP'].apply(
            pd.Timestamp.fromtimestamp)
        data_df['HEADER_TIME_STAMP_ORIGINAL'] = data_df[
            'HEADER_TIME_STAMP_ORIGINAL'].apply(pd.Timestamp.fromtimestamp)
        data_df['HEADER_TIME_STAMP_NOLOSS'] = data_df[
            'HEADER_TIME_STAMP_NOLOSS'].apply(pd.Timestamp.fromtimestamp)
        data_df['HEADER_TIME_STAMP_REAL'] = data_df[
            'HEADER_TIME_STAMP_REAL'].apply(pd.Timestamp.fromtimestamp)
        output_file = os.path.join(logging_folder, stream_name + '.sensor.csv')
        if not os.path.exists(output_file):
            data_df.to_csv(
                output_file, index=False, float_format='%.3f', mode='w')
        else:
            data_df.to_csv(
                output_file,
                index=False,
                float_format='%.3f',
                mode='a',
                header=False)


def _save_features(feature_df, stream_names, logging_folder):
    output_file = os.path.join(logging_folder,
                               '-'.join(stream_names) + '.feature.csv')
    if not os.path.exists(output_file):
        feature_df.to_csv(
            output_file, index=False, float_format='%.3f', mode='w')
    else:
        feature_df.to_csv(
            output_file,
            index=False,
            float_format='%.3f',
            mode='a',
            header=False)


def _save_predictions(prediction_df, stream_names, logging_folder):
    output_file = os.path.join(logging_folder,
                               '-'.join(stream_names) + '.prediction.csv')
    if not os.path.exists(output_file):
        prediction_df.to_csv(
            output_file, index=False, float_format='%.3f', mode='w')
    else:
        prediction_df.to_csv(
            output_file,
            index=False,
            float_format='%.3f',
            mode='a',
            header=False)


def _compute_features(chunks):
    feature_dfs = []
    feature_json = {}
    for chunk in chunks:
        stream_name = chunk.get_stream_name()
        packages = chunk.get_packages()
        dfs = [package.to_dataframe() for package in packages]
        chunk_index = chunk.get_chunk_index()
        data_df = pd.concat(dfs, axis=0, ignore_index=True)
        clean_df = data_df[['X', 'Y', 'Z']]
        X = clean_df.values
        df = FeatureSet.location_matters(X, 50)
        columns = df.columns
        columns = [col + '_' + stream_name.upper() for col in columns]
        df.columns = columns
        df['START_TIME'] = pd.Timestamp.fromtimestamp(chunk.get_chunk_st())
        df['STOP_TIME'] = pd.Timestamp.fromtimestamp(chunk.get_chunk_et())
        df['CHUNK_INDEX'] = chunk_index
        feature_dfs.append(df)
        feature_json[stream_name.upper() + '_FEATURE'] = json.loads(
            df.to_json(orient='records'))

    feature_df = reduce(
        lambda x, y: x.merge(y, on=['START_TIME', 'STOP_TIME', 'CHUNK_INDEX']),
        feature_dfs)
    return feature_json, feature_df


def _make_predictions(feature_df, model_files):
    indexed_feature_df = feature_df.set_index(
        ['START_TIME', 'STOP_TIME', 'CHUNK_INDEX'])

    prediction_json = {}
    prediction_dfs = []
    for model_file in model_files:
        with open(model_file, 'rb') as mf:
            model_bundle = pickle.load(mf)
            feature_order = model_bundle['feature_order']
            ordered_df = indexed_feature_df.loc[:, feature_order]
            X = ordered_df.values
            name = model_bundle['name']
            class_labels = model_bundle['model'].classes_
            p_df = pd.DataFrame()
            try:
                scaled_X = model_bundle['scaler'].transform(X)
                scores = model_bundle['model'].predict_proba(scaled_X)[0]
            except:
                scores = len(class_labels) * [np.nan]
            for class_label, score in zip(class_labels, scores):
                p_df[name + '_' + class_label.upper() + '_PREDICTION'] = [
                    score
                ]
            p_df['START_TIME'] = feature_df['START_TIME']
            p_df['STOP_TIME'] = feature_df['STOP_TIME']
            p_df['CHUNK_INDEX'] = feature_df['CHUNK_INDEX']
            prediction_dfs.append(p_df)
            prediction_json[name + '_PREDICTION'] = json.loads(
                p_df.to_json(orient='records'))
    prediction_df = reduce(
        lambda x, y: x.merge(y, on=['START_TIME', 'STOP_TIME', 'CHUNK_INDEX']),
        prediction_dfs)
    print(prediction_df)
    return prediction_json, prediction_df
