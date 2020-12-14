'''
This script cuts the audio file into smaller wav file of talkturns
'''

import datetime
import os

import numpy as np
import pandas as pd
from pydub import AudioSegment

import Python.Data_Preprocessing.config.config as cfg
import Python.Data_Preprocessing.config.dir_config as prs


def convert(n_sec):
    '''
    convert seconds to time format
    :param n: seconds
    :return: time
    '''
    return str(datetime.timedelta(seconds=n_sec))

def cut_to_talkturn(file, start_time, end_time, talkturn_no):
    '''
    cut wav file into talkturn wav files
    :param file: wav file
    :param start_time: start cut time
    :param end_time: end cut time
    :param talkturn_no: talkturn number
    :return: none
    '''
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")

    t_1 = start_time * 1000
    t_2 = end_time * 1000
    new_audio = AudioSegment.from_wav(os.path.join(parallel_run_settings['wav_path'], file))
    new_audio = new_audio[t_1:t_2]
    new_audio.export(os.path.join(parallel_run_settings['talkturn_wav_path'], file[:-4] + "_" +
                                  str(talkturn_no) + '.wav'), format="wav")

def cut_files(video_name_1, video_name_2):
    '''
    cut wav files into smaller wav talkturns
    :return: none
    '''
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    dfr = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                   video_name_1 + '_' + video_name_2,
                                   'Stage_2',
                                   "weaved talkturns.csv"))
    dfr['audio_id'] = dfr.apply(lambda x: video_name_1
    if x['speaker'] == cfg.parameters_cfg['speaker_1']
    else video_name_2, axis=1)

    print(dfr)

    wav_list = [video_name_1+'.wav', video_name_2+'.wav']
    #for i in os.listdir(parallel_run_settings['wav_path']):
        #if i.endswith(".wav"):
            #wav_list.append(i)

    for i in wav_list:
        new_data = dfr.loc[np.where((dfr['audio_id'] == i[:-4]))]

        for index, row in new_data.iterrows():
            start_time = row['start time']
            end_time = row['end time']
            if start_time != end_time:
                talkturn_number = row['talkturn no']
                cut_to_talkturn(file=i, start_time=start_time, end_time=end_time,
                                talkturn_no=talkturn_number)
                print("\n Cut talkturn: " + str(talkturn_number))
            else:
                continue

if __name__ == '__main__':
    cut_files(video_name_1='zoom_F', video_name_2='zoom_M')
