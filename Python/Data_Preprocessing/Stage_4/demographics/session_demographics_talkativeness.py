# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 13:50:29 2019

@author: Joshua
"""

# Load libraries
import pandas as pd

pd.__version__

import os
import Python.Data_Preprocessing.config.config as cfg
import Python.Data_Preprocessing.Stage_4.demographics.session_level_wordcount as slw

from tqdm import tqdm


def z_score_per_dev_fold(df, col_name):

    # for loop
    df_result = pd.DataFrame(columns=["Video_ID", col_name +'_z'])
    df_train = df
    train_mil = cfg.parameters_cfg['d_'+col_name+'_mu']
    train_sd = cfg.parameters_cfg['d_'+col_name+'_sd']

    df_series = (df[col_name] - train_mil) / (train_sd)
    df_return = pd.DataFrame()
    df_return['Video_ID'] = df_train.video_id.apply(lambda x: x.split('_')[0])
    df_return[col_name + '_z'] = df_series

    df_result = df_result.append(df_return)
    return df_result

def get_z_bucket(z_series):
    '''
    takes in the z_series and outputs the bucket very low;low;high;very high
    '''

    df = pd.DataFrame(z_series)
    df.columns = ['z']

    df['z_bucket'] = None
    df.loc[df.z < -1, 'z_bucket'] = 'low'
    df.loc[df.z < -2, 'z_bucket'] = 'very low'
    df.loc[df.z > 1, 'z_bucket'] = 'high'
    df.loc[df.z > 2, 'z_bucket'] = 'very high'


    return df

def run_dataframe(video_name_1, video_name_2, parallel_run_settings):
    '''
    Load dataframe of summary metrics
    :return: dataframe of metrics
    '''
    # parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    slw.run_computing_wordcount(video_name_1, video_name_2, parallel_run_settings)
    data = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                    video_name_1 + '_' + video_name_2,
                                    'Stage_4',
                                    'Demographics',
                                    'session_level_wordcount.csv'))
    return data

def get_blob(variable, fxn, speaker, data):
    '''
    Generates blob
    :param variable: specific AU variable being analyzed
    :param fxn: mean/std/min/max
    :param speaker: speaker 1 or speaker 2
    :return:
    '''
    var = z_score_per_dev_fold(df=data.loc[(data['speaker'] == speaker)],
                               col_name=variable+'_'+fxn)
    var = pd.concat([var, get_z_bucket(var[variable+'_'+fxn+'_z'])], axis=1)

    if fxn == 'count':
        text = 'number of '
    elif fxn == 'uniquecount':
        text = 'unique number of '
    elif fxn == 'countprop':
        text = 'proportion of number of '
    else:
        text = ''

    variable_text = 'words'

    print('Generating ', text+' '+variable_text+' by '+speaker+' ')
    i = 0
    df_copy = var.copy()
    df_copy['blob'] = None
    for i in tqdm(range(len(var))):
        row_df = var.iloc[i]
        if row_df['z_bucket'] == None:
            pass
        else:
            # Mark - change the formula that removes warning
            df_copy['blob'] = text + ' ' + variable_text + ' by ' + speaker + ' ' + row_df['z_bucket'] + ' '
            # df_copy['blob'] = df_copy.z_bucket.apply(lambda x: text + ' ' + variable_text + ' by ' + speaker + ' ' + x + ' ')
            print(df_copy)

    return df_copy


def get_all_blob(video_name_1, video_name_2, parallel_run_settings):
    # Importing connection object
    data = run_dataframe(video_name_1, video_name_2, parallel_run_settings)

    talkativeness_blob = pd.DataFrame()
    speakers = [cfg.parameters_cfg['speaker_1'], cfg.parameters_cfg['speaker_2']]

    for speaker in speakers:
        for fxn in ['count', 'countprop', 'uniquecount']:
            for variable_text in ['word']:
                blob = get_blob(variable_text, fxn, speaker, data)
                talkativeness_blob = pd.concat([talkativeness_blob,
                                      blob], axis=0)

        talkativeness_blob.fillna(value='', inplace=True)

    talkativeness_blob = talkativeness_blob.groupby(['Video_ID'])[
        'blob'].apply(lambda x: ''.join(x)).reset_index()

    for i in tqdm(range(len(talkativeness_blob))):
        row_df = talkativeness_blob.iloc[i]
        if not row_df['blob']:
            pass
        else:
            if len(row_df['blob']) > 0:
                talkativeness_blob.blob.iloc[i] = row_df['blob'][:-1] + '. '

        talkativeness_blob = talkativeness_blob.sort_values(by=['Video_ID'])

    return talkativeness_blob

if __name__ == '__main__':
    talkativeness_blob = get_all_blob(video_name_1='Ses01F_F',
                                      video_name_2='Ses01F_M')
