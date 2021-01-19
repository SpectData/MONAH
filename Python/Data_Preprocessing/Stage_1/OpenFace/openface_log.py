'''
This script logs the changes done in each video file during open face implementation
'''

import os

import pandas as pd

import Python.Data_Preprocessing.Stage_1.Audio_files_manipulation.copy_mp4_files as cmf
import Python.Data_Preprocessing.config.dir_config as prs


def update_row(src_dir, video_id, column_name):
    '''
    update row in the log table
    :param src_dir: local source of the log table
    :param video_id: video to be updated
    :param column_name: column to be updated
    :return: none
    '''
    assert column_name in set(['Opened_with_OpenFace',
                               'CSV_produced',
                               'VideoLength_checked']), "column_name must match"

    # log updates in a local file
    dfr = pd.read_csv(os.path.join(src_dir,
                                   "openface_log.csv"))
    dfr.loc[(dfr.Video_ID == video_id), [column_name]] = 1
    dfr.to_csv(os.path.join(src_dir,
                            "openface_log.csv"), index=False)

def batch_update_csv_produced(processed_files, src_dir):
    '''
    updates csv when open face is implemented
    :param processed_files: files done
    :param src_dir: local source of the log table
    :return: none
    '''
    i = 0
    for i in range(0, len(processed_files), 1):
        processed_file = processed_files.iloc[i]
        file_name = processed_file['file_name']

        # Logging Variables
        column_name = 'Opened_with_OpenFace'
        video_id = file_name[:-4]

        update_row(src_dir, video_id, column_name)

        column_name = 'CSV_produced'
        update_row(src_dir, video_id, column_name)

def openface_log(src_dir, video_id, column_name, parallel_run_settings):
    '''
    Log processes in open face execution
    :param video_id: video being analyzed
    :param column_name: column to be updated
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')

    update_row(src_dir, video_id, column_name)

    processed_files = cmf.get_local_list_files(file_format='avi')
    batch_update_csv_produced(processed_files, parallel_run_settings['csv_path'])

if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    openface_log(src_dir=os.path.join(parallel_run_settings['csv_path'],
                                      'zoom_F' + '_' + 'zoom_M',
                                      'Stage_1'),
                 video_id="zoom_M",
                 column_name='Opened_with_OpenFace')
