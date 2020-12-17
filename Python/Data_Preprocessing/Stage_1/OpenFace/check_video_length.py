'''
This script checks whether video length matches between what's obtained from OpenFace
and the recorded original video length
'''

import os

import pandas as pd

import Python.Data_Preprocessing.Stage_1.OpenFace.download_avi as da
import Python.Data_Preprocessing.Stage_1.OpenFace.openface_log as ofl


def check_videolength(video_name_1, video_name_2, parallel_run_settings):
    '''
    Checks matching of video length
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    video_list = da.download_video(video_name_1, video_name_2, parallel_run_settings)

    column_name = 'VideoLength_checked'
    for video_id in video_list:
        video_name = video_id[:-4]
        data = pd.read_csv(os.path.join(parallel_run_settings['OpenFace_CSV_Path'],
                                        video_name, video_name + ".csv"))
        timestamp = data[' timestamp'].max()
        data = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        'Stage_1',
                                        "video_duration.csv"))
        duration = data[data['Video_ID'] == video_name]['duration'].values[0]
        if abs(timestamp - duration) < 0.5:
            ofl.update_row(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        'Stage_1'), video_name, column_name)
        else:
            continue

if __name__ == '__main__':
    check_videolength(video_name_1, video_name_2)
