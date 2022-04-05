'''
Main manager for formulating session-level transcript
'''

import os

import pandas as pd
from datetime import datetime
import Python.Data_Preprocessing.Stage_3.main as m
import Python.Data_Preprocessing.Stage_4.actions.session_action_parent as sap
import Python.Data_Preprocessing.Stage_4.create_directories as cdi
import Python.Data_Preprocessing.Stage_4.demographics.session_demographics_parent as sdp
import Python.Data_Preprocessing.Stage_4.prosody.session_prosody_parent as spp
import Python.Data_Preprocessing.config.dir_config as prs


def weave_session_level_transcript(video_name_1,
                                   video_name_2,
                                   d_word_count,
                                   p_delay,
                                   p_wpm,
                                   p_tone,
                                   a_au,
                                   a_posiface,
                                   a_smile,
                                   parallel_run_settings):
    '''
    combine session level transcripts
    :param video_name_1:
    :param video_name_2:
    :return:
    '''
    start = datetime.now()
    cdi.run_creating_directories(video_name_1, video_name_2, parallel_run_settings)
    sdp.get_demographics_blob(video_name_1, video_name_2, parallel_run_settings, word_count=d_word_count)
    spp.get_prosody_blob(video_name_1, video_name_2, parallel_run_settings, delay=p_delay, wpm=p_wpm, tone=p_tone)
    sap.get_actions_blob(video_name_1, video_name_2, parallel_run_settings, au_action=a_au, posiface=a_posiface, smile=a_smile)

    demographics_blob = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                                 video_name_1 + '_' + video_name_2,
                                                 'Stage_4',
                                                 'Demographics',
                                                 'narrative_coarse.csv'))
    prosody_blob = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                                 video_name_1 + '_' + video_name_2,
                                                 'Stage_4',
                                                 'Prosody',
                                                 'narrative_coarse.csv'))
    actions_blob = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                                 video_name_1 + '_' + video_name_2,
                                                 'Stage_4',
                                                 'Actions',
                                                 'narrative_coarse.csv'))

    narrative_coarse = pd.concat([demographics_blob, prosody_blob, actions_blob], axis=0)
    narrative_coarse.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                         video_name_1 + '_' + video_name_2,
                                         'Stage_4',
                                         'narrative_coarse.csv'), index=False)
    print('Stage 4 Total Time: ', datetime.now() - start)

if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    weave_session_level_transcript(video_name_1='Ses04F_impro02_F',
                                   video_name_2='Ses04F_impro02_M',
                                   d_word_count=1,
                                   p_delay=1,
                                   p_wpm=1,
                                   p_tone=1,
                                   a_au=1,
                                   a_posiface=1,
                                   a_smile=1,
                                   parallel_run_settings=parallel_run_settings
                                   )
