'''
This script returns a list of wav files to be analyzed
'''

import os

import Python.Data_Preprocessing.config.dir_config as prs


def download_audio(video_name_1, video_name_2):
    '''
    Lists the .wav files from the source folder
    :param dest_dir: folder where the files are saved
    :return: list of names of .wav files
    '''
    parallel_run_settings = prs.get_parallel_run_settings('marriane_win')

    # List all audio downloaded
    audio_list = [video_name_1+'.wav', video_name_2+'.wav']
    for i in os.listdir(parallel_run_settings['wav_path']):
        if i.endswith(".wav") and i in audio_list:
            audio_list.append(i)

    return audio_list


if __name__ == '__main__':
    AUDIO_LIST = download_audio(video_name_1, video_name_2)
    print(AUDIO_LIST)
