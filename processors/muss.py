from padar_features.feature_set import FeatureSet
import pandas as pd
import signal
import asyncio
from padar_realtime.processor_stream import ProcessorStreamManager
from padar_realtime.file_stream import FileStream
from functools import reduce
import os
import json
import pickle
import numpy as np
import logging
from glob import glob

dir_path = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'pretrained_models')
PREDEFINED_MODELS = [
    os.path.join(dir_path, f) for f in [
        'DW_DA_DT.MO.thirteen_activities_model.pkl',
    ]
]


def get_model_filepath(model_name):
    return os.path.join('processors', 'pretrained_models', model_name + '.pkl')


def entry(merged,
          model_name,
          logging_folder=False,
          number_of_windows=None):
    model_file = get_model_filepath(model_name)
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
            feature_df, model_file)
        if logging_folder == False or (count >= number_of_windows
                                       and number_of_windows > 0):
            logging.info('skip data logging')
        else:
            _save_raw_data(chunks, logging_folder)
            _save_features(
                feature_df, stream_names, model_name, logging_folder)
            _save_predictions(
                prediction_df, stream_names, model_name, logging_folder)
        result_json = {**feature_json, **prediction_json}
        result_json['START_TIME'] = st
        result_json['STOP_TIME'] = et
        result_json['INDEX'] = count
        json_str = json.dumps(result_json)
        logging.debug(json_str)
        return json_str


def get_mhealth_filepath(df, logging_folder, stream_name, device_id, data_type='AccelerationCalibrated', file_type='sensor'):
    if 'HEADER_TIME_STAMP' in df:
        timestamp = df['HEADER_TIME_STAMP'][0]
    elif 'START_TIME' in df:
        timestamp = df['START_TIME'][0]
    timestamp = timestamp.strftime('%Y-%m-%d-%H-%M-%S-%f')
    timestamp = timestamp[:-3] + "-P0000"
    output_filepath = os.path.join(
        logging_folder, stream_name + '-' + data_type + '-NA' + '.' + device_id.replace(':', '') + '-' + data_type + '.' + timestamp + '.' + file_type + '.csv')
    return output_filepath


def convert_unix_ts_column_to_timestamp(df, columns):
    for col in columns:
        df[col] = df[col].apply(pd.Timestamp.fromtimestamp)
    return df


def _save_raw_data(chunks, logging_folder):
    for chunk in chunks:  # each input stream
        stream_name = chunk.get_stream_name()
        device_id = chunk.get_device_id()
        packages = chunk.get_packages()
        chunk_index = chunk.get_chunk_index()
        dfs = [package.to_dataframe() for package in packages]
        data_df = pd.concat(dfs, axis=0, ignore_index=True)
        data_df['CHUNK_INDEX'] = chunk_index
        data_df = convert_unix_ts_column_to_timestamp(data_df, columns=[
            'HEADER_TIME_STAMP', 'HEADER_TIME_STAMP_ORIGINAL', 'HEADER_TIME_STAMP_NOLOSS', 'HEADER_TIME_STAMP_REAL'])
        if chunk_index == 0:
            output_filepath = get_mhealth_filepath(
                data_df, logging_folder, stream_name, device_id)
            file_streamer = FileStream(filename=output_filepath)
            file_streamer.start()
            file_streamer.write(data_df)
            file_streamer.stop()
        else:
            output_filepath = glob(
                os.path.join(logging_folder, stream_name + '*.sensor.csv'))[0]
            file_streamer = FileStream(filename=output_filepath)
            file_streamer.start()
            file_streamer.write(data_df)
            file_streamer.stop()


def _save_features(feature_df, stream_names, model_name, logging_folder):
    chunk_index = feature_df['CHUNK_INDEX'][0]
    if chunk_index == 0:
        output_filepath = get_mhealth_filepath(
            feature_df, logging_folder, '_'.join(stream_names), model_name.upper().replace('.', '_'), 'MussOM', 'feature')
        file_streamer = FileStream(filename=output_filepath)
        file_streamer.start()
        file_streamer.write(feature_df)
        file_streamer.stop()
    else:
        output_filepath = glob(
            os.path.join(logging_folder,
                         '_'.join(stream_names) + '*.feature.csv'))[0]
        file_streamer = FileStream(filename=output_filepath)
        file_streamer.start()
        file_streamer.write(feature_df)
        file_streamer.stop()


def _save_predictions(prediction_df, stream_names, model_name, logging_folder):
    chunk_index = prediction_df['CHUNK_INDEX'][0]
    if chunk_index == 0:
        output_filepath = get_mhealth_filepath(
            prediction_df, logging_folder, '_'.join(stream_names), model_name.upper().replace('.', '_'), 'MussOM', 'prediction')
        file_streamer = FileStream(filename=output_filepath)
        file_streamer.start()
        file_streamer.write(prediction_df)
        file_streamer.stop()
    else:
        output_filepath = glob(
            os.path.join(logging_folder,
                         '_'.join(stream_names) + '*.prediction.csv'))[0]
        file_streamer = FileStream(filename=output_filepath)
        file_streamer.start()
        file_streamer.write(prediction_df)
        file_streamer.stop()


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


def _make_predictions(feature_df, model_file):
    indexed_feature_df = feature_df.set_index(
        ['START_TIME', 'STOP_TIME', 'CHUNK_INDEX'])

    prediction_json = {}
    prediction_dfs = []
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
