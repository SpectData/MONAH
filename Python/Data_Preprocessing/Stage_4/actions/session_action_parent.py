'''
This script combines all the action session level transcript
'''

# Load libraries
import pandas as pd

pd.__version__

import os

import Python.Data_Preprocessing.Stage_4.actions.session_action_posiface as sap
import Python.Data_Preprocessing.Stage_4.actions.session_action_smile as sas
import Python.Data_Preprocessing.Stage_4.actions.session_action_au as saa
import Python.Data_Preprocessing.config.dir_config as prs

def get_actions_blob(video_name_1, video_name_2, au_action, posiface, smile):
    '''
    Combining all action transcripts in one blob
    :param video_name_1:
    :param video_name_2:
    :return:
    '''
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    posiface_count_blob = sap.get_all_blob(video_name_1,
                                           video_name_2)
    smiling_count_blob = sas.get_all_blob(video_name_1,
                                          video_name_2)
    au_blob = saa.get_all_blob(video_name_1,
                               video_name_2)

    posiface_count_blob = posiface_count_blob.sort_values(by=['Video_ID'])
    posiface_count_blob['blob'] = posiface_count_blob.blob.apply(lambda x: x if posiface == 1 else '')

    smiling_count_blob = smiling_count_blob.sort_values(by=['Video_ID'])
    smiling_count_blob['blob'] = smiling_count_blob.blob.apply(lambda x: x if smile == 1 else '')

    au_blob = au_blob.sort_values(by=['Video_ID'])
    au_blob['blob'] = au_blob.blob.apply(lambda x: x if smile == 1 else '')

    len(set(posiface_count_blob.Video_ID))
    len(set(smiling_count_blob.Video_ID))
    len(set(au_blob.Video_ID))

    actions_blob = pd.merge(left=au_blob, right=smiling_count_blob,
                            on=['Video_ID'],
                            how='left')

    actions_blob.fillna(value='', inplace=True)

    actions_blob['actions_blob'] = actions_blob['blob_x'] + \
                                   actions_blob['blob_y']

    actions_blob = actions_blob[['Video_ID', 'actions_blob']]

    actions_blob = pd.merge(left=actions_blob, right=posiface_count_blob,
                            on=['Video_ID'],
                            how='left')

    actions_blob.fillna(value='', inplace=True)

    actions_blob['actions_blob'] = actions_blob['actions_blob'] + \
                                   actions_blob['blob']

    actions_blob = actions_blob[['Video_ID', 'actions_blob']]

    # EXPORT

    actions_blob['family'] = 'actions'
    export = actions_blob[['Video_ID', 'family', 'actions_blob']]
    export.columns = ['Video_ID', 'family', 'text_blob']

    export.to_csv(os.path.join(parallel_run_settings['csv_path'],
                               video_name_1 + '_' + video_name_2,
                               'Stage_4',
                               'Actions',
                               'narrative_coarse.csv'), index=False)

if __name__ == '__main__':
    get_actions_blob(video_name_1='Ses01F_F',
                     video_name_2='Ses01F_M',
                     au_action=1,
                     posiface=1,
                     smile=1)