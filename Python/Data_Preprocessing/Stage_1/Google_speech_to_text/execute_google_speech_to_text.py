'''
This script uses google speech to text to transcribe text from the wav files
'''

import os
import pathlib

import pandas as pd
import Python.Data_Preprocessing.config.dir_config as cfg
import Python.Data_Preprocessing.Stage_1.Google_speech_to_text.download_wav as dw
import Python.Data_Preprocessing.Stage_1.Google_speech_to_text.transcribe_wav_file as twf
import Python.Data_Preprocessing.Stage_1.Google_speech_to_text.upload_to_gcs as utg


def insert_to_table(dfr, dest_dir, table_name):
    '''
    :param df: data frame to be inserted to the file/table
    :param dest_dir: folder location of the file/table to append data
    :param table_name: file/table name to append data to
    :return: none
    '''
    dfr.to_csv(os.path.join(dest_dir, table_name + ".csv"), index=False)
    print("\nSuccessfully inserted data to " + table_name + ".csv")

def run_google_speech_to_text(video_1, video_2, parallel_run_settings):
    '''
    Run google speech to text
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    # Mark - add a condition that stops the function from running again if file exists
    if os.path.exists(str(pathlib.Path(os.path.join(parallel_run_settings['csv_path'],
                      video_1 + '_' + video_2, 'Stage_1', 'word_transcripts.csv')))):
        return print('Stage 1 Google Speech-to-Text File Exists')

    bucket = "marriane-bucket"
    audio_list = dw.download_audio(video_1, video_2)
    print(audio_list)
    utterance = pd.DataFrame()
    word = pd.DataFrame()
    for audio_id in audio_list:
        audio_full_path = os.path.join(parallel_run_settings['wav_path'], audio_id)
        utg.upload_blob(bucket_name=bucket,
                        destination_blob_name=audio_id,
                        parallel_run_settings=parallel_run_settings)
        result = twf.transcribe_gcs(bucket_name=bucket,
                                    audio_id=audio_id,
                                    parallel_run_settings=parallel_run_settings)
        utterance = utterance.append(result[0])
        word = word.append(result[1])

    dest_dir = os.path.join(parallel_run_settings['csv_path'],
                            video_1 + '_' + video_2, 'Stage_1')

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    insert_to_table(dfr=utterance,
                    dest_dir=dest_dir,
                    table_name="utterance_transcripts")
    insert_to_table(dfr=word,
                    dest_dir=dest_dir,
                    table_name="word_transcripts")

if __name__ == '__main__':
    parallel_run_settings = cfg.get_parallel_run_settings("marriane_win")
    run_google_speech_to_text(video_1="zoom_F",
                              video_2="zoom_M",
                              parallel_run_settings=parallel_run_settings)
