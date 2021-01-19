'''
This script consolidates all the video files and return it as a list
'''

import os

import pandas as pd
from tqdm import tqdm


def get_local_list_files(video_name_1, video_name_2, file_format, parallel_run_settings):
    '''
    List video files to be analyzed
    :param file_format:
    :param src_dir:
    :return: list of video file names
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    print('Get Local List Files', file_format)
    assert file_format in set(['avi']), 'file_format must be avi'
    full_paths = []
    file_names = []
    video_ids = []
    speakers = []
    file_sizes = []
    video_list = [video_name_1+'.avi', video_name_2+'.avi']

    for root, dirs, files in tqdm(os.walk(parallel_run_settings['avi_path'])):
        for file in files:
            if file.endswith('.' + file_format) and file in video_list:
                full_path = os.path.abspath(os.path.join(root, file))
                full_paths.append(full_path)
                file_names.append(file)
                file_sizes.append(os.path.getsize(full_path))
                video_ids.append(file[:-4])

                if '_F' in file:
                    speakers.append('F')
                else:
                    speakers.append('M')

    results = pd.DataFrame(list(zip(full_paths, file_names, video_ids, speakers, file_sizes)),
                           columns=['full_path', 'file_name', 'Video_ID', 'Speaker', 'file_size'])

    return results


def run_creating_directories(video_name_1, video_name_2, parallel_run_settings):
    '''
    Create needed directories
    :param video_name_1: file 1
    :param video_name_2: file 2
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    out_dir = os.path.join(parallel_run_settings['csv_path'],
                           video_name_1 + '_' + video_name_2)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    for sub_folder in ['Stage_1', 'Stage_2', 'Stage_3', 'Stage_4']:
        sub_dir = os.path.join(out_dir, sub_folder)
        if not os.path.exists(sub_dir):
            os.makedirs(sub_dir)

if __name__ == '__main__':
    LOCAL_FILES = get_local_list_files(video_name_1='zoom_F',
                                       video_name_2='zoom_M',
                                       file_format="avi")
    print(LOCAL_FILES)
    run_creating_directories(video_name_1='zoom_F', video_name_2='zoom_M')
