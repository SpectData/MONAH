'''
Main manager for formulating session-level transcript
'''

import os
import pandas as pd
import data.secrets.parallel_run_settings_secret as prs
import Python.Data_Preprocessing.Stage_4.create_directories as cdi
import Python.Data_Preprocessing.Stage_4.actions.session_action_parent as sap
import Python.Data_Preprocessing.Stage_4.prosody.session_prosody_parent as spp
import Python.Data_Preprocessing.Stage_4.demographics.session_demographics_parent as sdp

def weave_session_level_transcript(video_name_1,
                                   video_name_2,
                                   d_word_count,
                                   p_delay,
                                   p_wpm,
                                   p_tone,
                                   a_au,
                                   a_posiface,
                                   a_smile):
    '''
    combine session level transcripts
    :param video_name_1:
    :param video_name_2:
    :return:
    '''
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    cdi.run_creating_directories(video_name_1, video_name_2)
    sdp.get_demographics_blob(video_name_1, video_name_2, word_count=d_word_count)
    spp.get_prosody_blob(video_name_1, video_name_2, delay=p_delay, wpm=p_wpm, tone=p_tone)
    sap.get_actions_blob(video_name_1, video_name_2, au_action=a_au, posiface=a_posiface, smile=a_smile)

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

if __name__ == '__main__':
    weave_session_level_transcript(video_name_1='Ses01F_F',
                                   video_name_2='Ses01F_M',
                                   d_word_count=1,
                                   p_delay=1,
                                   p_wpm=1,
                                   p_tone=1,
                                   a_au=1,
                                   a_posiface=1,
                                   a_smile=1
                                   )
