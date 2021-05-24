'''
This script creates a summary table of all action features of a video
'''
import os

import pandas as pd
from datetime import datetime
import Python.Data_Preprocessing.config.dir_config as prs

import Python.Data_Preprocessing.Stage_2.Actions.talkturn_au_actions as aua
import Python.Data_Preprocessing.Stage_2.Actions.talkturn_posiface as psf
import Python.Data_Preprocessing.Stage_2.Actions.talkturn_smile as sml
import Python.Data_Preprocessing.Stage_2.Actions.talkturn_head_nod as hnd
import Python.Data_Preprocessing.Stage_2.Actions.talkturn_lean_forward as lfd


def combine_actions_features(video_name_1, video_name_2, gstt, parallel_run_settings):
    '''
    combine action features in one summary table
    :return: none
    '''
    action_start = datetime.now()
    start = datetime.now()
    sml.compute_smile(video_name_1, video_name_2, gstt, parallel_run_settings=parallel_run_settings)
    print('Stage 2 Action Smile Time: ', datetime.now() - start)

    start = datetime.now()
    psf.compute_posiface(video_name_1, video_name_2, gstt, parallel_run_settings=parallel_run_settings)
    print('Stage 2 Action Posiface Time: ', datetime.now() - start)

    start = datetime.now()
    aua.compute_au_actions(video_name_1, video_name_2, gstt, parallel_run_settings=parallel_run_settings)
    print('Stage 2 Action AUs Time: ', datetime.now() - start)

    start = datetime.now()
    hnd.compute_head_nod(video_name_1, video_name_2, gstt, parallel_run_settings=parallel_run_settings)
    print('Stage 2 Action HeadNod Time: ', datetime.now() - start)

    start = datetime.now()
    lfd.compute_lean_forward(video_name_1, video_name_2, gstt, parallel_run_settings=parallel_run_settings)
    print('Stage 2 Action ForwardLean Time: ', datetime.now() - start)

    # load dataframes
    if gstt == 0:
        talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        "Stage_2",
                                        "weaved talkturns.csv"))
    else:
        talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                            video_name_1 + '_' + video_name_2,
                                            "Stage_2",
                                            "weaved talkturns_gstt.csv"))
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
    #head nod detected
    head_nod = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                      video_name_1 + "_" + video_name_2,
                                      "Stage_2",
                                      "talkturn_headnod.csv"))
    head_nod['audio_id'] = head_nod['video_id'].apply(lambda x: x.split('_')[0])
    head_nod = head_nod[['audio_id', 'speaker', 'talkturn no', 'headnod']]
    # lean forward detected
    lean_forward = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + "_" + video_name_2,
                                        "Stage_2",
                                        "talkturn_leanforward.csv"))
    lean_forward['audio_id'] = lean_forward['video_id'].apply(lambda x: x.split('_')[0])
    lean_forward = lean_forward[['audio_id', 'speaker', 'talkturn no', 'leanforward']]

    dfs = [smile, posiface, au_actions, head_nod, lean_forward]
    dfr = dfs[0]
    for df_ in dfs[1:]:
        dfr = pd.merge(dfr, df_, how='outer', on=['audio_id', 'speaker', 'talkturn no'])

    dfr = dfr.fillna(0)
    dfr.to_csv(os.path.join(parallel_run_settings['csv_path'],
                            video_name_1 + "_" + video_name_2,
                            "Stage_2",
                            'talkturn_family_actions.csv'),
               index=False)

    print('Stage 2 Action Time: ', datetime.now() - action_start)

if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    combine_actions_features(video_name_1='Ses01F_F',
                             video_name_2='Ses01F_M',
                             gstt=1,
                             parallel_run_settings=parallel_run_settings)
