'''
This script creates a table of positive face expression from the videos
'''
import os
import pathlib
import numpy as np
import pandas as pd
from datetime import datetime
import Python.Data_Preprocessing.config.config as cfg


def compute_posiface(video_1, video_2, parallel_run_settings):
    '''
    Compute for posiface status of the talkturn
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    # Mark - add a condition that stops the function from running again if file exists
    if os.path.exists(str(pathlib.Path(os.path.join(parallel_run_settings['csv_path'], video_1 + '_' + video_2, 'Stage_2', 'talkturn_posiface.csv')))):
        return print('Stage 2 Action - Posiface Exists')

    start = datetime.now()
    # Load dateframes
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_1 + '_' + video_2,
                                        "Stage_2",
                                        "weaved talkturns.csv"))
    open_face_results = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                                 video_1 + '_' + video_2,
                                                 "Stage_1",
                                                 "openface_raw.csv"))
    open_face_results['speaker'] = open_face_results.apply(lambda x:
                                                           cfg.parameters_cfg['speaker_1']
                                                           if x['video_id'] == video_1 else
                                                           cfg.parameters_cfg['speaker_2'],
                                                           axis=1)
    open_face_results = open_face_results.sort_values(by=['video_id', 'frame'])
    # Mark - Select the needed fields in openface_raw.csv to reduce memory consumption
    open_face_results = open_face_results[['video_id','speaker','frame',' timestamp',' AU12_c']]
    for_posiface = pd.merge(talkturn, open_face_results, how="inner", on=["video_id", "speaker"])
    for_posiface['time_status'] = np.where((for_posiface['start time'] <=
                                            for_posiface[' timestamp']) &
                                           (for_posiface['end time'] >=
                                            for_posiface[' timestamp']), 1, 0)
    for_posiface = for_posiface[for_posiface['time_status'] == 1]
    for_posiface['posiface_status'] = np.where((for_posiface[' AU12_c'] == 1), 1, 0)
    posiface = for_posiface.groupby(['video_id', 'speaker', 'talkturn no']).agg({
        'posiface_status': sum,
        ' timestamp': 'count'
    })
    posiface = posiface.reset_index()
    posiface['posiface'] = np.where((posiface['posiface_status'] == posiface[' timestamp']), 1, 0)
    posiface.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                 video_1 + "_" + video_2,
                                 "Stage_2",
                                 "talkturn_posiface.csv"),
                    index=False)
    print('Stage 2 Action Posiface Time: ', datetime.now() - start)

if __name__ == '__main__':
    compute_posiface(video_1='Ses01F_F',
                     video_2='Ses01F_M',
                     parallel_run_settings=parallel_run_settings)
