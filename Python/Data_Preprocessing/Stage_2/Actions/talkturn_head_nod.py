'''
This script creates a table of head nodding instances from the videos
'''
import os

import numpy as np
import pandas as pd

import Python.Data_Preprocessing.config.config as cfg
import Python.Data_Preprocessing.config.dir_config as prs


def compute_head_nod(video_name_1, video_name_2, parallel_run_settings):
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
        for var in ['frame', ' timestamp', ' y_30']:
            for mult in [1, -1]:
                if mult == -1:
                    m = 'p'
                else:
                    m = 'm'
                open_face_results[var+'_'+m+str(i)] = open_face_results.groupby('video_id')[var].shift(i*mult)

    # unpivot
    df1 = open_face_results.melt(id_vars=['video_id', 'frame', ' face_id', ' timestamp', ' y_30'],
                                 value_vars=['frame_m0', 'frame_m1', 'frame_m2', 'frame_m3',
                                             'frame_m4', 'frame_p1', 'frame_p2', 'frame_p3',
                                             'frame_p4'],
                                 var_name='frame_b',
                                 value_name='value')
    df1['period'] = df1.frame_b.apply(lambda x: x[-2:])
    df2 = open_face_results.melt(id_vars=['video_id', 'frame', ' face_id', ' timestamp', ' y_30'],
                                 value_vars=[' timestamp_m0', ' timestamp_m1', ' timestamp_m2',
                                             ' timestamp_m3', ' timestamp_m4', ' timestamp_p1',
                                             ' timestamp_p2', ' timestamp_p3', ' timestamp_p4'],
                                 var_name='timestamp_b',
                                 value_name='value')
    df2['period'] = df2.timestamp_b.apply(lambda x: x[-2:])
    df3 = open_face_results.melt(id_vars=['video_id', 'frame', ' face_id', ' timestamp', ' y_30'],
                                 value_vars=[' y_30_m0', ' y_30_m1', ' y_30_m2',
                                             ' y_30_m3', ' y_30_m4', ' y_30_p1',
                                             ' y_30_p2', ' y_30_p3', ' y_30_p4'],
                                 var_name='y_30_b',
                                 value_name='value')
    df3['period'] = df3.y_30_b.apply(lambda x: x[-2:])

    # join tables
    df = pd.merge(df1, df2, how='inner', on=['video_id', 'frame', ' face_id', ' timestamp', 'period', ' y_30'])
    df = pd.merge(df, df3, how='inner', on=['video_id', 'frame', ' face_id', ' timestamp', 'period', ' y_30'])
    df = df.drop(['value_x', 'value_y', 'period', 'y_30_b'], axis=1)
    df.columns = ['video_id', 'frame', 'face_id', 'timestamp', 'y_30', 'frame_b', 'timestamp_b', 'y_30_b']

    # get peak points (min or max)
    helper_1 = df.groupby(['video_id', 'frame', 'face_id', 'timestamp', 'y_30']).agg({'y_30_b': ['min', 'max']}).reset_index()
    helper_1.columns = ['video_id', 'frame', 'face_id', 'timestamp', 'y_30', 'y_30_b_min', 'y_30_b_max']

    # identify state of each peak point (stable or transient)
    helper_1['state_a'] = helper_1.apply(lambda x: 'stable' if x['y_30_b_max'] - x['y_30_b_min'] <= 2 else 'transient', axis = 1)

    # find extremes from each cycle
    helper_1['state_a2'] = helper_1.apply(lambda x: 'extreme' if x['state_a'] == 'transient' and
                                                                                ((x['y_30'] == x['y_30_b_min']) or (x['y_30'] == x['y_30_b_max']))
                                                                                 else x['state_a'], axis = 1)
    extreme = helper_1[helper_1['state_a2'] == 'extreme']
    extreme['extreme_num'] = extreme.groupby('video_id')['frame'].rank(method="first", ascending=True)

    # get previous stable state of each stable state
    stable = helper_1[helper_1['state_a2'] == 'stable']
    stable = stable.sort_values(by=['video_id', 'frame'])
    stable['previous_stable_timestamp'] = stable.groupby('video_id')['timestamp'].shift(1)
    stable['stable_num'] = stable.groupby('video_id')['frame'].rank(method="first", ascending=True)

    # merge base table to helper table
    base = pd.merge(helper_1, stable, how = 'left', on = ['video_id', 'frame'])
    base = base[['video_id', 'frame', 'face_id_x', 'timestamp_x', 'y_30_x', 'y_30_b_min_x',
                'y_30_b_max_x', 'state_a_x', 'state_a2_x', 'previous_stable_timestamp']]

    # find stable cycles having at least two consecutive extremes in between
    base_2 = pd.DataFrame()
    for index, row in base.iterrows():
        video_id = row['video_id']
        timestamp = row['timestamp_x']
        previous_stable_timestamp = row['previous_stable_timestamp']

        eligible = base.loc[(base['video_id'] == video_id) &
                            (base['timestamp_x'] <= timestamp) &
                            (base['timestamp_x'] >= previous_stable_timestamp)]
        eligible = eligible[['state_a2_x', 'frame']]
        eligible.columns = ['state_b', 'frame_b']
        eligible['video_id'] = video_id
        eligible['frame'] = row['frame']
        eligible['face_id'] = row['face_id_x']
        eligible['timestamp'] = timestamp
        eligible['y_30_a'] = row['y_30_x']
        eligible['state_a'] = row['state_a2_x']
        eligible['previous_stable_timestamp'] = previous_stable_timestamp
        base_2 = base_2.append(eligible)

    base_2['extreme'] = base_2.state_b.apply(lambda x: 1 if x == 'extreme' else 0)
    base_3 = base_2.groupby(['video_id', 'frame', 'face_id', 'timestamp', 'y_30_a', 'state_a',
                             'previous_stable_timestamp']).agg({'extreme': sum}).reset_index()
    with_extremes = base_3[base_3['extreme'] > 2]

    # get only cycles with extremes
    base_4 = pd.DataFrame()
    for index, row in with_extremes.iterrows():
        video_id = row['video_id']
        timestamp = row['timestamp']
        previous_stable_timestamp = row['previous_stable_timestamp']
        eligible = helper_1.loc[(helper_1['video_id'] == video_id) &
                                (helper_1['timestamp'] <= timestamp) &
                                (helper_1['timestamp'] >= previous_stable_timestamp) &
                                (helper_1['state_a2'] != 'transient')]
        base_4 = base_4.append(eligible)

    # put unique identifier per cycle
    stable = pd.merge(base_4[base_4['state_a2'] == 'stable'], stable, how='inner',
                      on=['video_id', 'frame'])
    stable = stable[['video_id', 'frame', 'face_id_x', 'timestamp_x', 'y_30_x', 'y_30_b_min_x',
                     'y_30_b_max_x', 'state_a_x', 'state_a2_x', 'previous_stable_timestamp']]
    stable.columns = ['video_id', 'frame', 'face_id', 'timestamp', 'y_30', 'y_30_b_min',
                      'y_30_b_max', 'state_a', 'state_a2', 'previous_stable_timestamp']
    stable['group_num'] = (stable.index / 2 + 1).astype(int)
    temp_extreme = base_4[base_4['state_a2'] == 'extreme']

    extreme = pd.DataFrame()
    for index, row in stable.iterrows():
        video_id = row['video_id']
        timestamp = row['timestamp']
        previous_stable_timestamp = row['previous_stable_timestamp']
        group_num = row['group_num']
        eligible = temp_extreme[(temp_extreme['video_id'] == video_id) &
                                (temp_extreme['timestamp'] <= timestamp) &
                                (temp_extreme['timestamp'] >= previous_stable_timestamp)]
        eligible['previous_stable_timestamp'] = 00.00000
        eligible['group_num'] = group_num
        extreme = extreme.append(eligible)

    base_5 = stable.append(extreme)

    # select cycles with more than 2 unique extremes
    temp = base_5[base_5['state_a2'] == 'extreme']
    temp = temp.groupby(['video_id', 'group_num']).y_30.nunique().reset_index()
    temp = temp[temp['y_30'] > 2]
    base_6 = pd.merge(base_5, temp, how='inner', on=['video_id', 'group_num'])
    base_6 = base_6[['video_id', 'frame', 'face_id', 'timestamp', 'y_30_x', 'y_30_b_min',
                      'y_30_b_max', 'state_a', 'state_a2', 'previous_stable_timestamp', 'group_num']]
    base_6.columns = ['video_id', 'frame', 'face_id', 'timestamp', 'y_30', 'y_30_b_min',
                      'y_30_b_max', 'state_a', 'state_a2', 'previous_stable_timestamp', 'group_num']
    base_6 = base_6.sort_values(by=['video_id', 'group_num', 'frame'])

    # select cycles with consecutive extremes varying by at least 16 pixels
    temp = base_6[base_6['state_a2'] == 'extreme']
    temp['previous_y_30'] = temp.groupby(['video_id', 'group_num'])['y_30'].shift(1)
    temp['eligible_extreme'] = temp.apply(lambda x: 1 if abs(x['y_30'] - x['previous_y_30']) > 16
                                          else 0, axis=1)
    temp = temp.groupby(['video_id', 'group_num']).agg({'eligible_extreme': sum}).reset_index()
    temp = temp[temp['eligible_extreme'] > 1]

    # combine all final tables
    base_7 = pd.merge(base_6, temp, how='inner', on=['video_id', 'group_num'])
    base_7 = base_7[base_7['state_a2'] == 'stable']
    base_7 = base_7.groupby(['video_id', 'group_num']).agg({'timestamp': [min, max]}).reset_index()
    base_7.columns = ['video_id', 'group_num', 'start_time', 'end_time']
    base_7 = base_7[(base_7['end_time'] - base_7['start_time'] >= 1) &
                    (base_7['end_time'] - base_7['start_time'] <= 1.4)]
    base_7['speaker'] = base_7.apply(lambda x: cfg.parameters_cfg['speaker_1']
    if x['video_id'] == video_name_1 else cfg.parameters_cfg['speaker_2'], axis=1)

    for_head_nod = pd.merge(talkturn, base_7, how="left", on=["video_id", "speaker"])
    for_head_nod['time_status'] = np.where((for_head_nod['start time'] <=
                                            for_head_nod['start_time']) &
                                           (for_head_nod['end time'] >=
                                            for_head_nod['end_time']), 1, 0)
    for_head_nod['headnod'] = np.where((for_head_nod['time_status'] == 1), 1, 0)
    headnod = for_head_nod[['video_id', 'speaker', 'talkturn no', 'headnod']]
    headnod.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                video_name_1 + "_" + video_name_2,
                                "Stage_2",
                                "talkturn_headnod.csv"),
                   index=False)

if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    compute_head_nod(video_name_1='Ses01F_F',
                     video_name_2='Ses01F_M',
                     parallel_run_settings=parallel_run_settings)
