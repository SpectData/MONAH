'''
This script computes for pitch per talkturn
'''
import os

import numpy as np
import pandas as pd

import Python.Data_Preprocessing.config.dir_config as prs


def extract_delay(video_name_1, video_name_2, parallel_run_settings):
    '''
    Computes for the talkturn delay
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        "Stage_2",
                                        'weaved talkturns.csv'))
    talkturn['previous_speaker'] = talkturn['speaker'].shift()
    talkturn['next_start_time'] = talkturn['start time'].shift(-1)
    talkturn['delay'] = talkturn['next_start_time'] - talkturn['end time']
    talkturn['speaker_match'] = np.where((talkturn['speaker'] != talkturn['previous_speaker']),
                                         0, 1)
    idx = np.where((talkturn['speaker_match'] == 0) & (talkturn['delay'] > 0))
    final_talkturn = talkturn.loc[idx]
    final_talkturn.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                       video_name_1 + '_' + video_name_2,
                                       "Stage_2",
                                       'talkturn_delay.csv'),
                          index=False)


if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('joshua_linux')

    video_name_1 = 'Ses01F_F'
    video_name_2 = 'Ses01F_M'

    extract_delay(video_name_1='Ses01F_F', video_name_2='Ses01F_M')
