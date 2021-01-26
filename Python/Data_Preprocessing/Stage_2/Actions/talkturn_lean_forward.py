'''
This script creates a table of head nodding instances from the videos
'''
import os

import numpy as np
import pandas as pd

import Python.Data_Preprocessing.config.config as cfg
import Python.Data_Preprocessing.config.dir_config as prs


def compute_lean_forward(video_name_1, video_name_2, parallel_run_settings):
    '''
    Compute for head nod status of the talkturn
    :return: none
    '''
    # Load dataframes
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        "Stage_2",
                                        "weaved talkturns.csv"))
    open_face_results = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                                 video_name_1 + '_' + video_name_2,
                                                 "Stage_1",
                                                 "openface_raw.csv"))
    open_face_results['speaker'] = open_face_results.apply(lambda x:
                                                           cfg.parameters_cfg['speaker_1']
                                                           if x['video_id'] == video_name_1 else
                                                           cfg.parameters_cfg['speaker_2'],
                                                           axis=1)
    open_face_results = open_face_results.sort_values(by=['video_id', 'frame'])

    # get lead and lag values
    for i in range(0,5):
        for var in ['frame', ' timestamp', ' pose_Tz']:
            for mult in [1, -1]:
                if mult == -1:
                    m = 'p'
                else:
                    m = 'm'
                open_face_results[var+'_'+m+str(i)] = open_face_results.groupby('video_id')[var].shift(i*mult)

    # unpivot
    df1 = open_face_results.melt(id_vars=['video_id', 'frame', ' face_id', ' timestamp', ' pose_Tz'],
                                 value_vars=['frame_m0', 'frame_m1', 'frame_m2', 'frame_m3',
                                             'frame_m4', 'frame_p1', 'frame_p2', 'frame_p3',
                                             'frame_p4'],
                                 var_name='frame_b',
                                 value_name='value')
    df1['period'] = df1.frame_b.apply(lambda x: x[-2:])
    df2 = open_face_results.melt(id_vars=['video_id', 'frame', ' face_id', ' timestamp', ' pose_Tz'],
                                 value_vars=[' timestamp_m0', ' timestamp_m1', ' timestamp_m2',
                                             ' timestamp_m3', ' timestamp_m4', ' timestamp_p1',
                                             ' timestamp_p2', ' timestamp_p3', ' timestamp_p4'],
                                 var_name='timestamp_b',
                                 value_name='value')
    df2['period'] = df2.timestamp_b.apply(lambda x: x[-2:])
    df3 = open_face_results.melt(id_vars=['video_id', 'frame', ' face_id', ' timestamp', ' pose_Tz'],
                                 value_vars=[' pose_Tz_m0', ' pose_Tz_m1', ' pose_Tz_m2',
                                             ' pose_Tz_m3', ' pose_Tz_m4', ' pose_Tz_p1',
                                             ' pose_Tz_p2', ' pose_Tz_p3', ' pose_Tz_p4'],
                                 var_name='pose_Tz_b',
                                 value_name='value')
    df3['period'] = df3.pose_Tz_b.apply(lambda x: x[-2:])

    # join tables
    df = pd.merge(df1, df2, how='inner', on=['video_id', 'frame', ' face_id', ' timestamp', 'period', ' pose_Tz'])
    df = pd.merge(df, df3, how='inner', on=['video_id', 'frame', ' face_id', ' timestamp', 'period', ' pose_Tz'])
    df = df.drop(['value_x', 'value_y', 'period', 'pose_Tz_b'], axis=1)
    df.columns = ['video_id', 'frame', 'face_id', 'timestamp', 'pose_Tz', 'frame_b', 'timestamp_b', 'pose_Tz_b']

    # get peak points (min or max)
    helper_1 = df.groupby(['video_id', 'frame', 'face_id', 'timestamp', 'pose_Tz']).agg({'pose_Tz_b': ['min', 'max']}).reset_index()
    helper_1.columns = ['video_id', 'frame', 'face_id', 'timestamp', 'pose_Tz', 'pose_Tz_b_min', 'pose_Tz_b_max']

    # identify state of each peak point (stable or transient)
    helper_1['state_a'] = helper_1.apply(lambda x: 'stable' if x['pose_Tz_b_max'] - x['pose_Tz_b_min'] <= 2 else 'transient', axis = 1)

    # get previous stable state of each stable state
    stable = helper_1[helper_1['state_a'] == 'stable']
    stable = stable.sort_values(by=['video_id', 'frame'])
    stable['previous_stable_timestamp'] = stable.groupby('video_id')['timestamp'].shift(1)
    stable['previous_stable_pose_Tz'] = stable.groupby('video_id')['pose_Tz'].shift(1)
    stable['stable_num'] = stable.groupby('video_id')['frame'].rank(method="first", ascending=True)

    # select cycles with stable points varying by at least 8cm
    stable['eligible'] = stable.apply(lambda x: 1 if x['previous_stable_pose_Tz'] - x['pose_Tz'] >= 80
    else 0, axis=1)
    stable['magnitude'] = stable.apply(lambda x: x['previous_stable_pose_Tz'] - x['pose_Tz'], axis=1)
    stable['speaker'] = stable.apply(lambda x: cfg.parameters_cfg['speaker_1']
    if x['video_id'] == video_name_1 else cfg.parameters_cfg['speaker_2'], axis=1)

    base = stable[['video_id', 'speaker', 'previous_stable_timestamp', 'timestamp', 'eligible', 'magnitude']]
    base.columns = ['video_id', 'speaker', 'start_time', 'end_time', 'eligible', 'magnitude']

    for_lean_forward = pd.merge(talkturn, base, how="left", on=["video_id", "speaker"])
    for_lean_forward['time_status'] = np.where((for_lean_forward['start time'] <=
                                                for_lean_forward['start_time']) &
                                               (for_lean_forward['end time'] >=
                                                for_lean_forward['end_time']), 1, 0)
    for_lean_forward = for_lean_forward[for_lean_forward['time_status'] == 1]
    for_lean_forward = for_lean_forward.groupby(['video_id', 'audio_id', 'speaker', 'talkturn no',
                                                 'text', 'start time', 'end time']).agg({
        'eligible': sum, 'time_status': sum
    }).reset_index()
    for_lean_forward['leanforward'] = np.where((for_lean_forward['eligible'] > 0), 1, 0)
    leanforward = pd.merge(talkturn, for_lean_forward, how="left", on=["video_id", "speaker", "talkturn no"])
    leanforward = leanforward[['video_id', 'speaker', 'talkturn no', 'leanforward']]
    leanforward['leanforward'] = leanforward.leanforward.apply(lambda x: 1 if x == 1 else 0)
    leanforward.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                video_name_1 + "_" + video_name_2,
                                "Stage_2",
                                "talkturn_leanforward.csv"),
                   index=False)

if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    compute_lean_forward(video_name_1='Ses01F_F',
                         video_name_2='Ses01F_M',
                         parallel_run_settings=parallel_run_settings)
