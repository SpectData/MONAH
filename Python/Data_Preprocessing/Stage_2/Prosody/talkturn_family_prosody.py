'''
This script creates a summary table of all prosody features of a video
'''
import os
import pathlib

import numpy as np
import pandas as pd
from datetime import datetime

import Python.Data_Preprocessing.Stage_2.Prosody.talkturn_delay as dly
import Python.Data_Preprocessing.Stage_2.Prosody.talkturn_wpm as wpm


def get_summary_feature(table_name, column_name):
    '''
    This function gets the mean and standard deviation per talkturn
    :param table_name: table to be analyzed
    :param column_name: column to be analyzed
    :return: summary table of mean and sd
    '''
    dfr = pd.merge(table_name, table_name, how="inner", on=['audio_id', 'speaker'])
    dfr['previous_talkturn'] = np.where(dfr['talkturn no_y'] < dfr['talkturn no_x'], 1, 0)
    dfr[column_name] = dfr[column_name + '_y'] * dfr['previous_talkturn']
    df_summary = dfr.groupby(['audio_id', 'speaker', 'talkturn no_x']).agg({
        column_name: ['mean', 'std']
    })
    df_summary = df_summary.reset_index()
    df_summary.columns = ['audio_id', 'speaker', 'talkturn no',
                          column_name + '_mean', column_name + '_sd']

    return df_summary

def normalize_column_values(table_name, column_name, summary_table):
    '''
    This function normalizes the given column values
    :param table_name: table to be normalized
    :param column_name: column to be normalized
    :param summary_table: table of means and standard deviation
    :return: table with normalized column values
    '''
    dfr = pd.merge(table_name, summary_table, how="inner",
                   on=['audio_id', 'speaker', 'talkturn no'])
    dfr[column_name + '_z'] = np.where(dfr[column_name + '_sd'] == 0, 0,
                                       (dfr[column_name] - dfr[column_name + '_mean'])*1.0/
                                       dfr[column_name + '_sd'])
    dfr = dfr[['audio_id', 'speaker', 'talkturn no', column_name + '_z']]

    return dfr

def combine_prosody_features(video_name_1, video_name_2, parallel_run_settings):
    '''
    Combine normalize feature values
    :return: none
    '''

    # video_name_1 = video_1
    # video_name_2 = video_2
    # Mark - add a condition that stops the function from running again if file exists
    #if os.path.exists(str(pathlib.Path(os.path.join(parallel_run_settings['csv_path'], video_name_1 + '_' + video_name_2, 'Stage_2', 'talkturn_family_prosody.csv')))):
    #    return print('Stage 2 Prosody File Exists')

    prosody_start = datetime.now()
    wpm.extract_speech_rate(video_name_1, video_name_2, parallel_run_settings=parallel_run_settings)
    dly.extract_delay(video_name_1, video_name_2, parallel_run_settings=parallel_run_settings)

    # Load dataframes
    df_wpm = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                      video_name_1 + '_' + video_name_2,
                                      "Stage_2",
                                      "talkturn_wpm.csv"))
    df_delay = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        "Stage_2",
                                        "talkturn_delay.csv"))
    df_delay['delay_ms'] = df_delay['delay'] * 1000
    df_vokaturi = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                           video_name_1 + '_' + video_name_2,
                                           "Stage_1",
                                           "talkturn_vokaturi.csv"))

    wpm_summary = get_summary_feature(table_name=df_wpm, column_name="wpm")
    delay_summary = get_summary_feature(table_name=df_delay, column_name="delay")
    vokaturi_summary_1 = get_summary_feature(table_name=df_vokaturi, column_name="neutrality")
    vokaturi_summary_2 = get_summary_feature(table_name=df_vokaturi, column_name="happiness")
    vokaturi_summary_3 = get_summary_feature(table_name=df_vokaturi, column_name="sadness")
    vokaturi_summary_4 = get_summary_feature(table_name=df_vokaturi, column_name="anger")
    vokaturi_summary_5 = get_summary_feature(table_name=df_vokaturi, column_name="fear")

    wpm_summary = normalize_column_values(table_name=df_wpm,
                                          column_name="wpm",
                                          summary_table=wpm_summary)
    delay_summary = normalize_column_values(table_name=df_delay,
                                            column_name="delay",
                                            summary_table=delay_summary)
    delay_summary = pd.merge(delay_summary, df_delay, how='inner',
                             on=['audio_id', 'speaker', 'talkturn no'])
    delay_summary = delay_summary[['audio_id', 'speaker', 'talkturn no', 'delay_z', 'delay_ms']]
    vokaturi_summary_1 = normalize_column_values(table_name=df_vokaturi,
                                                 column_name="neutrality",
                                                 summary_table=vokaturi_summary_1)
    vokaturi_summary_2 = normalize_column_values(table_name=df_vokaturi,
                                                 column_name="happiness",
                                                 summary_table=vokaturi_summary_2)
    vokaturi_summary_3 = normalize_column_values(table_name=df_vokaturi,
                                                 column_name="sadness",
                                                 summary_table=vokaturi_summary_3)
    vokaturi_summary_4 = normalize_column_values(table_name=df_vokaturi,
                                                 column_name="anger",
                                                 summary_table=vokaturi_summary_4)
    vokaturi_summary_5 = normalize_column_values(table_name=df_vokaturi,
                                                 column_name="fear",
                                                 summary_table=vokaturi_summary_5)

    dfs = [wpm_summary, delay_summary, vokaturi_summary_1, vokaturi_summary_2,
           vokaturi_summary_3, vokaturi_summary_4, vokaturi_summary_5]
    dfr = dfs[0]
    for df_ in dfs[1:]:
        dfr = pd.merge(dfr, df_, how='outer', on=['audio_id', 'speaker', 'talkturn no'])

    dfr = dfr.fillna(0)
    dfr.to_csv(os.path.join(parallel_run_settings['csv_path'],
                            video_name_1 + '_' + video_name_2,
                            "Stage_2",
                            'talkturn_family_prosody.csv'),
               index=False)

    print('Stage 2 Prosody Time: ', datetime.now() - prosody_start)

if __name__ == '__main__':
    combine_prosody_features(video_name_1='zoom_F',
                             video_name_2='zoom_M')
