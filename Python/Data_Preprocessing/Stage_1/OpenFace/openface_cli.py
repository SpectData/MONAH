'''
This script sets up open face loop invoke process
'''

import datetime
import os
import pathlib

import Python.Data_Preprocessing.Stage_1.Audio_files_manipulation.copy_mp4_files as cmf
import Python.Data_Preprocessing.Stage_1.OpenFace.openface_log as ofl


def invoke_openface_cli(video_full_path, video_file_name, feature_extraction_path, out_dir):
    '''
    Sets up open face subprocess
    :param video_full_path: full path of video file
    :param video_file_name: video file name
    :param FeatureExtractionPath: openface path
    :param out_dir: destination directory
    :return:
    '''
    bash_command = feature_extraction_path + "/FeatureExtraction -f "
    bash_command = bash_command + video_full_path

    # Create output directory
    bash_command = bash_command + ' -out_dir '
    subfolder = video_file_name[0:-4]
    out_dir = os.path.join(out_dir, subfolder)
    bash_command = bash_command + out_dir
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    print(bash_command)

    # Invoke OpenFace
    import subprocess
    c_p = subprocess.run(bash_command.split(),
                         check=True,
                         universal_newlines=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    print(c_p.stderr, c_p.returncode)
    return c_p

def get_todo_files(file_info):
    '''
    Inner Joins file_info with yet to be done video_id from the DB
    :param file_info: contain the df of local files
    :return: yet to be done video_ids
    '''
    return file_info

def for_loop_invoke_cli(video_name_1, video_name_2, parallel_run_settings):
    '''
    implement open face run
    :param parallel_run_settings: folder settings
    :param file_info: all files
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    file_info = cmf.get_local_list_files(video_name_1, video_name_2, file_format='avi',
                                         parallel_run_settings=parallel_run_settings)

    file_info = file_info.sort_values(by=['Video_ID'])
    print(len(file_info), len(file_info) / 2)

    feature_extraction_path = parallel_run_settings['feature_extraction_path']
    out_dir = parallel_run_settings['OpenFace_CSV_Path']
    src_dir = parallel_run_settings['csv_path']

    for i in range(0, len(file_info), 1):
        print('\n===============================================')
        todo_file_info = get_todo_files(file_info)
        start_time = datetime.datetime.now()

        f_i = file_info.iloc[i]
        video_full_path = pathlib.Path(f_i['full_path'])
        video_file_name = f_i['file_name']
        print(i, 'of', len(file_info), start_time, video_full_path)

        if todo_file_info['file_name'].str.contains(video_file_name).any():
            # Pre-processing Logging
            column_name = 'Opened_with_OpenFace'
            video_id = f_i['Video_ID']
            ofl.update_row(os.path.join(src_dir,
                                        video_name_1 + '_' + video_name_2,
                                        'Stage_1'), video_id, column_name)

            # Start Processing
            c_p = invoke_openface_cli(str(video_full_path),
                                      video_file_name,
                                      feature_extraction_path,
                                      out_dir)
            print('cp.stderr length', len(c_p.stderr))

            # Post processing Logging
            if not c_p.stderr:

                column_name = 'CSV_produced'
                ofl.update_row(os.path.join(src_dir,
                                            video_name_1 + '_' + video_name_2,
                                            'Stage_1'), video_id, column_name)
        else:
            print('This file is not in todo_file_info.')

        # Record time delta
        end_time = datetime.datetime.now()
        print('time taken:', end_time-start_time)

if __name__ == '__main__':
    for_loop_invoke_cli(video_name_1='zoom_F', video_name_2='zoom_M')
