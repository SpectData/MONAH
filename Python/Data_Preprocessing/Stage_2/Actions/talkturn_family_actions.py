'''
This script creates a summary table of all action features of a video
'''
import os
import numpy as np
import pandas as pd
import data.secrets.parallel_run_settings_secret as prs
import Python.Data_Preprocessing.Stage_2.Actions.talkturn_smile as sml
import Python.Data_Preprocessing.Stage_2.Actions.talkturn_posiface as psf
import Python.Data_Preprocessing.Stage_2.Actions.talkturn_au_actions as aua

def combine_actions_features(video_name_1, video_name_2):
    '''
    combine action features in one summary table
    :return: none
    '''
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    sml.compute_smile(video_name_1, video_name_2)
    psf.compute_posiface(video_name_1, video_name_2)
    aua.compute_au_actions(video_name_1, video_name_2)

    # load dataframes
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + "_" + video_name_2,
                                        "Stage_2",
                                        "weaved talkturns.csv"))
    talkturn = talkturn[['audio_id', 'speaker', 'talkturn no', 'text', 'start time', 'end time']]
    # smile detected
    smile = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                     video_name_1 + "_" + video_name_2,
                                     "Stage_2",
                                     "talkturn_smile.csv"))
    smile['audio_id'] = smile['video_id'].apply(lambda x: x.split('_')[0])
    smile = smile[['audio_id', 'speaker', 'talkturn no', 'smile']]
    # posiface detected
    posiface = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + "_" + video_name_2,
                                        "Stage_2",
                                        "talkturn_posiface.csv"))
    posiface['audio_id'] = posiface['video_id'].apply(lambda x: x.split('_')[0])
    posiface = posiface[['audio_id', 'speaker', 'talkturn no', 'posiface']]
    # au actions detected
    au_actions = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                          video_name_1 + "_" + video_name_2,
                                          "Stage_2",
                                          "talkturn_au_actions.csv"))
    au_actions['audio_id'] = au_actions['video_id'].apply(lambda x: x.split('_')[0])
    au_actions = au_actions[['audio_id', 'speaker', 'talkturn no', 'AU05_c',
                             'AU17_c', 'AU20_c', 'AU25_c']]

    dfs = [smile, posiface, au_actions]
    dfr = dfs[0]
    for df_ in dfs[1:]:
        dfr = pd.merge(dfr, df_, how='outer', on=['audio_id', 'speaker', 'talkturn no'])

    dfr = dfr.fillna(0)
    dfr.to_csv(os.path.join(parallel_run_settings['csv_path'],
                            video_name_1 + "_" + video_name_2,
                            "Stage_2",
                            'talkturn_family_actions.csv'),
               index=False)

if __name__ == '__main__':
    combine_actions_features(video_name_1='Ses01F_F', video_name_2='Ses01F_M')
