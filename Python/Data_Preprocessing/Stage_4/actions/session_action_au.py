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
    train_mil = cfg.parameters_cfg['a_'+col_name+'_mu']
    train_sd = cfg.parameters_cfg['a_'+col_name+'_sd']

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
    raw_data = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                    video_name_1 + '_' + video_name_2,
                                    'Stage_1',
                                    'openface_raw.csv'))
    raw_data = raw_data[['video_id', ' timestamp', ' AU05_r', ' AU17_r', ' AU20_r', ' AU25_r']]
    talkurn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                       video_name_1 + '_' + video_name_2,
                                       'Stage_2',
                                       'weaved talkturns.csv'))
    data = pd.merge(talkurn, raw_data, how = "inner", on = "video_id")
    data['time_status'] = data.apply(lambda x: 1 if x['start time'] <= x[' timestamp'] and
    x[' timestamp'] <= x['end time'] else 0, axis=1)
    data = data.loc[(data['time_status']==1)]
    data = data.groupby(['video_id', 'audio_id', 'speaker']).agg(
        {' AU05_r': [min, max, 'mean', 'std'], ' AU17_r': [min, max, 'mean', 'std'],
         ' AU20_r': [min, max, 'mean', 'std'], ' AU25_r': [min, max, 'mean', 'std']
         }
    )
    data = data.reset_index()
    data.columns = ['video_id', 'audio_id', 'speaker',
                    'AU05_min', 'AU05_max', 'AU05_mean', 'AU05_std',
                    'AU17_min', 'AU17_max', 'AU17_mean', 'AU17_std',
                    'AU20_min', 'AU20_max', 'AU20_mean', 'AU20_std',
                    'AU25_min', 'AU25_max', 'AU25_mean', 'AU25_std']
    data['AU05_va'] = data.AU05_std.apply(lambda x: x*x)
    data['AU17_va'] = data.AU17_std.apply(lambda x: x*x)
    data['AU20_va'] = data.AU20_std.apply(lambda x: x*x)
    data['AU25_va'] = data.AU25_std.apply(lambda x: x*x)

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

    if variable == 'AU05':
        variable_text = 'upper lid raiser'
    elif variable == 'AU17':
        variable_text = 'chin raiser'
    elif variable == 'AU20':
        variable_text = 'lip stretcher'
    elif variable == 'AU25':
        variable_text = 'jaw drop'
    else:
        variable_text = ''

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

    action_blob = pd.DataFrame()
    speakers = [cfg.parameters_cfg['speaker_1'], cfg.parameters_cfg['speaker_2']]

    for speaker in speakers:
        for fxn in ['mean', 'va', 'min', 'max']:
            for variable_text in ['AU05', 'AU17', 'AU20', 'AU25']:
                blob = get_blob(variable_text, fxn, speaker, data)
                print(blob)
                action_blob = pd.concat([action_blob,
                                      blob], axis=0)

    action_blob.fillna(value='', inplace=True)

    action_blob = action_blob.groupby(['Video_ID'])[
        'blob'].apply(lambda x: ''.join(x)).reset_index()

    for i in tqdm(range(len(action_blob))):
        row_df = action_blob.iloc[i]
        if not row_df['blob']:
            pass
        else:
            if len(row_df['blob']) > 0:
                action_blob.blob.iloc[i] =  row_df['blob'][:-1] + '. '

        action_blob = action_blob.sort_values(by=['Video_ID'])

    return action_blob


if __name__ == '__main__':
    AUs_blob = get_all_blob(video_name_1='Ses01F_F',
                            video_name_2='Ses01F_M')
