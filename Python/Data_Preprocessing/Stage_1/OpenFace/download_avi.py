'''
This script lists all the video files to be analyzed
'''
import os

import pandas as pd
from moviepy.editor import VideoFileClip

import Python.Data_Preprocessing.config.dir_config as prs


def get_video_duration(src_dir, video_list, dest_dir):
    '''
    Computes for video duration of the video file
    :param src_dir: local file directory
    :param video_list: list of videos
    :param dest_dir: destination file to be updated
    :return:
    '''
    row_list = []
    for video_i in video_list:
        input_file = os.path.join(src_dir, video_i)
        clip = VideoFileClip(input_file)
        video_duration = clip.duration
        clip.close()
        row_i = {'Video_ID': video_i[:-4], 'duration': video_duration}
        row_list.append(row_i)
        print("Computed duration: " + video_i)

    # Store duration
    dfr = pd.DataFrame(row_list)
    dfr.to_csv(os.path.join(dest_dir, "video_duration.csv"))

    # For open face
    dfr['Opened_with_OpenFace'] = 0
    dfr['CSV_produced'] = 0
    dfr['VideoLength_checked'] = 0
    dfr.to_csv(os.path.join(dest_dir, "openface_log.csv"))

    print('Inserted video duration of video list')

def download_video(video_name_1, video_name_2):
    '''
    Downloads local videos to be analyzed
    :return: video list
    '''
    parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    video_list = [video_name_1+'.avi', video_name_2+'.avi']
    get_video_duration(src_dir=parallel_run_settings['avi_path'],
                       video_list=video_list,
                       dest_dir=os.path.join(parallel_run_settings['csv_path'],
                                             video_name_1 + '_' + video_name_2,
                                             'Stage_1'))

    return video_list

if __name__ == '__main__':
    download_video(video_name_1='zoom_F', video_name_2='zoom_M')
