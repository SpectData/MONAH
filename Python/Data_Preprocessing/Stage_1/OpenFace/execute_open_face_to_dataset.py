'''
This script runs open face to all the videos
'''
import os
import pathlib
import pandas as pd

import Python.Data_Preprocessing.Stage_1.Audio_files_manipulation.copy_mp4_files as cmf
import Python.Data_Preprocessing.Stage_1.OpenFace.check_video_length as cvl
import Python.Data_Preprocessing.Stage_1.OpenFace.download_avi as da
import Python.Data_Preprocessing.Stage_1.OpenFace.openface_cli as ofc
import data.secrets.parallel_run_settings_secret as prs


def run_open_face(video_name_1, video_name_2, parallel_run_settings):
    '''
    Run open face to video files
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    # Mark - add a condition that stops the function from running again if file exists
    if os.path.exists(str(pathlib.Path(os.path.join(parallel_run_settings['csv_path'], video_name_1 + '_' + video_name_2, 'Stage_1', 'openface_raw.csv')))):
        return print('Stage 1 OpenFace Raw File Exists')

    # download videos
    video_list = da.download_video(video_name_1, video_name_2, parallel_run_settings)

    #  get video duration
    da.get_video_duration(src_dir=parallel_run_settings['avi_path'],
                          video_list=video_list,
                          dest_dir=os.path.join(parallel_run_settings['csv_path'],
                                                video_name_1 + '_' + video_name_2,
                                                'Stage_1'))

    # run open face
    file_info = cmf.get_local_list_files(video_name_1, video_name_2, file_format='avi',
                                         parallel_run_settings=parallel_run_settings)
    file_info = file_info.sort_values(by=['Video_ID'])
    print(len(file_info), len(file_info) / 2)
    ofc.for_loop_invoke_cli(video_name_1, video_name_2, parallel_run_settings=parallel_run_settings)
    # check video duration
    cvl.check_videolength(video_name_1, video_name_2, parallel_run_settings=parallel_run_settings)

    # combine open face results
    open_face_results = pd.DataFrame()
    for video_i in video_list:
        results = pd.read_csv(os.path.join(parallel_run_settings['OpenFace_CSV_Path'] +
                                           "/" + video_i[:-4], video_i[:-4] + ".csv"))
        results['video_id'] = video_i[:-4]
        open_face_results = open_face_results.append(results)

    open_face_results.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                          video_name_1 + '_' + video_name_2,
                                          'Stage_1',
                                          "openface_raw.csv"),
                             index=False)

if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    run_open_face(video_name_1="Ses01F_F", video_name_2="Ses01F_M", parallel_run_settings=parallel_run_settings)
