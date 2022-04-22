'''
This script creates a table of smile detected from the videos
'''
import os
import pathlib

import numpy as np
import pandas as pd
from datetime import datetime

import Python.Data_Preprocessing.config.config as cfg


def compute_smile(video_1, video_2, parallel_run_settings):
    '''
    Compute smile per talkturn
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    # Mark - add a condition that stops the function from running again if file exists
    if os.path.exists(str(pathlib.Path(os.path.join(parallel_run_settings['csv_path'], video_1 + '_' + video_2, 'Stage_2', 'talkturn_smile.csv')))):
        return print('Stage 2 Action - Smile Exists')

    start = datetime.now()
    # Load files
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_1 + '_' + video_2,
                                        "Stage_2",
                                        "weaved talkturns.csv"))
    open_face_results = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                                 video_1 + '_' + video_2,
                                                 "Stage_1",
                                                 "openface_raw.csv"))
    open_face_results['speaker'] = open_face_results.apply(lambda x:
                                                           cfg.parameters_cfg['speaker_1']
                                                           if x['video_id'] == video_1 else
                                                           cfg.parameters_cfg['speaker_2'],
                                                           axis=1)
    open_face = open_face_results[(open_face_results[' AU06_c'] == 1) &
                                  (open_face_results[' AU12_c'] == 1)]
    open_face = open_face.sort_values(by=['video_id', 'frame'])
    open_face = open_face[['video_id', 'speaker', 'frame', ' timestamp', ' AU06_c', ' AU12_c']]
    open_face.columns = ['video_id', 'speaker', 'frame', 'timestamp', 'AU06_c', 'AU12_c']
    open_face['previous_timestamp'] = open_face['timestamp'].shift()
    open_face['previous_frame'] = open_face['frame'].shift()
    open_face['frame_status'] = open_face.apply(lambda x: x['frame'] - x['previous_frame'],
                                                axis=1)
    smiling_df = open_face[(open_face['frame_status'] != 1) | (open_face['frame_status'].isnull())]
    smiling_df['previous_smiling_end_date'] = smiling_df.previous_timestamp.apply(lambda x: x)
    smiling_df['start_date'] = smiling_df.timestamp.apply(lambda x: x)
    smiling_df['end_date'] = smiling_df.previous_smiling_end_date.shift(-1)
    smiling_df = smiling_df[(smiling_df['end_date'] - smiling_df['start_date']) >=
                            cfg.parameters_cfg['a_smile_time']]
    smiling_df = smiling_df[['video_id', 'speaker', 'start_date', 'end_date']]
    smiling_df = pd.merge(smiling_df, talkturn, how="right", on=['video_id', 'speaker'])
    smiling_df['smile'] = np.where((smiling_df['start time'] <= smiling_df['start_date']) &
                                   (smiling_df['end time'] >= smiling_df['end_date']), 1, 0)
    # Mark - aggregate the smile at talkturn level
    smiling_df = smiling_df.groupby(['video_id', 'speaker', 'talkturn no'])['smile'].max().reset_index()
    smiling_df = smiling_df[['video_id', 'speaker', 'talkturn no', 'smile']]
    smiling_df.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                   video_1 + "_" + video_2,
                                   "Stage_2",
                                   'talkturn_smile.csv'),
                      index=False)
    print('Stage 2 Action Smile Time: ', datetime.now() - start)

if __name__ == '__main__':
    compute_smile(video_1='Ses01F_F', video_2='Ses01F_M')
