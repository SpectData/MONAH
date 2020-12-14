'''
This script prepares the session-level transcript for au actions
'''

import os

import pandas as pd

import Python.Data_Preprocessing.config.dir_config as prs

pd.__version__

import Python.Data_Preprocessing.config.config as cfg

from tqdm import tqdm


def z_score_per_dev_fold(df, col_name):
    # for loop
    df_result = pd.DataFrame(columns=["Video_ID", col_name +'_z'])
    df_train = df
    train_mil = cfg.parameters_cfg['p_'+col_name+'_mu']
    train_sd = cfg.parameters_cfg['p_'+col_name+'_sd']

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

def run_dataframe(video_name_1, video_name_2):
    '''
    Load dataframe of summary metrics
    :return: dataframe of metrics
    '''
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    data = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                    video_name_1 + '_' + video_name_2,
                                    'Stage_2',
                                    'talkturn_wpm.csv'))
    data = data[['video_id', 'audio_id', 'speaker', 'talkturn no', 'wpm']]
    data = data.groupby(['video_id', 'audio_id', 'speaker']).agg(
        {'wpm': ['mean', 'std']}
    )
    data = data.reset_index()
    data.columns = ['video_id', 'audio_id', 'speaker', 'wpm_mean', 'wpm_std']
    data['wpm_va'] = data.wpm_std.apply(lambda x: x*x)

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

    if fxn == 'mean':
        text = 'average'
    elif fxn == 'va':
        text = 'variance'
    elif fxn == 'min':
        text = 'minimum'
    elif fxn == 'max':
        text = 'maximum'
    else:
        text = ''

    variable_text = 'speech rate'

    print('Generating ', text+' '+variable_text+' by '+speaker+' ')
    i = 0
    df_copy = var.copy()
    df_copy['blob'] = None
    for i in tqdm(range(len(var))):
        row_df = var.iloc[i]
        if row_df['z_bucket'] == None:
            pass
        else:
            df_copy.blob.iloc[i] = text+' ' +variable_text+' by '+speaker+' '+\
                                   row_df['z_bucket'] + ' '
            print(df_copy)

    return df_copy

def get_all_blob(video_name_1, video_name_2):
    # Importing connection object
    data = run_dataframe(video_name_1, video_name_2)

    speechrate_blob = pd.DataFrame()
    speakers = [cfg.parameters_cfg['speaker_1'], cfg.parameters_cfg['speaker_2']]

    for speaker in speakers:
        for fxn in ['mean', 'va']:
            for variable_text in ['wpm']:
                blob = get_blob(variable_text, fxn, speaker, data)
                print(blob)
                speechrate_blob = pd.concat([speechrate_blob,
                                      blob], axis=0)

    speechrate_blob.fillna(value='', inplace=True)

    speechrate_blob = speechrate_blob.groupby(['Video_ID'])[
        'blob'].apply(lambda x: ''.join(x)).reset_index()

    for i in tqdm(range(len(speechrate_blob))):
        row_df = speechrate_blob.iloc[i]
        if not row_df['blob']:
            pass
        else:
            if len(row_df['blob']) > 0:
                speechrate_blob.blob.iloc[i] =  row_df['blob'][:-1] + '. '

        speechrate_blob = speechrate_blob.sort_values(by=['Video_ID'])

    return speechrate_blob


if __name__ == '__main__':
    speechrate_blob = get_all_blob(video_name_1='Ses01F_F',
                              video_name_2='Ses01F_M')
