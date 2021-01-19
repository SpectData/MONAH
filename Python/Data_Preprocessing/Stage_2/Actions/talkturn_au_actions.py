'''
This script creates a table of AU actions from the videos
'''
import os

import numpy as np
import pandas as pd

import Python.Data_Preprocessing.config.config as cfg


def compute_au_actions(video_name_1, video_name_2, parallel_run_settings):
    '''
    Compute for the au summary per talkturn
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        "Stage_2",
                                        "weaved talkturns.csv"))
    open_face_results = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                                 video_name_1 + '_' + video_name_2,
                                                 "Stage_1",
                                                 "openface_raw.csv"))
    open_face_results['speaker'] = open_face_results.apply(lambda x:
                                                           cfg.parameters_cfg['speaker_1']
                                                           if x['video_id'] == video_name_1 else
                                                           cfg.parameters_cfg['speaker_2'],
                                                           axis=1)
    open_face_results = open_face_results.sort_values(by=['video_id', 'speaker', 'frame'])
    for_aus = pd.merge(talkturn, open_face_results, how="inner", on=["video_id", "speaker"])
    for_aus['time_status'] = np.where((for_aus['start time'] <=
                                       for_aus[' timestamp']) &
                                      (for_aus['end time'] >=
                                       for_aus[' timestamp']), 1, 0)
    for_aus = for_aus[for_aus['time_status'] == 1]
    au_actions = for_aus.groupby(['video_id', 'speaker', 'talkturn no']).agg({
        ' timestamp': 'count',
        ' AU05_c': sum,
        ' AU17_c': sum,
        ' AU20_c': sum,
        ' AU25_c': sum
    })
    au_actions = au_actions.reset_index()
    au_actions['AU05_c'] = np.where((au_actions[' timestamp'] == au_actions[' AU05_c']), 1, 0)
    au_actions['AU17_c'] = np.where((au_actions[' timestamp'] == au_actions[' AU17_c']), 1, 0)
    au_actions['AU20_c'] = np.where((au_actions[' timestamp'] == au_actions[' AU20_c']), 1, 0)
    au_actions['AU25_c'] = np.where((au_actions[' timestamp'] == au_actions[' AU25_c']), 1, 0)
    au_actions = au_actions[['video_id', 'speaker', 'talkturn no',
                             'AU05_c', 'AU17_c', 'AU20_c', 'AU25_c']]

    au_actions.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                   video_name_1 + '_' + video_name_2,
                                   'Stage_2',
                                   "talkturn_au_actions.csv"),
                      index=False)

if __name__ == '__main__':
    compute_au_actions(video_name_1='zoom_F', video_name_2='zoom_M')
