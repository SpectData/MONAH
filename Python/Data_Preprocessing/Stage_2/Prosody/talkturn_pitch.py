'''
This script computes for pitch per talkturn
'''
import glob
import os

import numpy as np
import pandas as pd
import parselmouth

import Python.Data_Preprocessing.config.dir_config as prs


def compute_speaker_pitch_summary(dfr):
    '''
    Compute mean and sd of pitch given the dataframe of wav paths
    :param dfr:
    :return:
    '''
    speaker_pitch = pd.DataFrame()

    for talkturn_idx in range(len(dfr)):
        talkturn_i = dfr.iloc[talkturn_idx]
        snd = parselmouth.Sound(talkturn_i['wav_path'])
        pitch = snd.to_pitch()
        pitch_values = pitch.selected_array['frequency']
        pitch_values[pitch_values == 0] = np.nan
        pitch_df = pd.DataFrame()
        pitch_df['time'] = pitch.xs()
        # the time is based on time in wav (start from zero for every talkturn)
        # We add the start time of everytalk turn to compute the time in conversation.
        pitch_df['time'] += talkturn_i['start time']
        pitch_df['F0'] = pitch_values

        # Drop silences that will affect sd and mean calculation
        pitch_df = pitch_df[pitch_df['F0'] > 0]

        if len(pitch_df) > 0:
            speaker_pitch = pd.concat([speaker_pitch, pitch_df], axis=0)

    mean = speaker_pitch['F0'].mean()
    sd = speaker_pitch['F0'].std()

    return mean, sd


def compute_coversation_pitch_summary(video_name_1, video_name_2, parallel_run_settings):
    '''
    We need the speaker-level pitch summary to understand when is considered high pitch
    for that speaker. Specifically, we need the mean and sd of pitch for that speaker.

    That means that we should iterate through all extracted talkturn wav fies, so that
    we consider only audio data from the talkturns f the speaker. We union the pitch data and
    compute sd and mean from this union pitch data.
    :param video_name_1:
    :param video_name_2:
    :param parallel_run_settings:
    :return:
    '''

    # Get the talkturn
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        "Stage_2",
                                        'weaved talkturns.csv'))

    talkturn['wav_name'] = talkturn['video_id'] + '_' + talkturn['talkturn no'].map(str) + '.wav'

    # List the wav talkturn files extracted
    wav_files = glob.glob(os.path.join(parallel_run_settings['talkturn_wav_path'], '*.wav'))
    wav_files = pd.DataFrame(wav_files)
    wav_files.columns = ['wav_path']

    # Extract the file name from the full path
    wav_files['wav_name'] = [os.path.basename(x) for x in wav_files['wav_path']]

    # Inner join between talkturn and wav_files will give us the talkturns that has wav associated
    merged = pd.merge(left=talkturn, right=wav_files, how='inner')

    # For each speaker, calculate the sd and mean of the pitch
    videos = [video_name_1, video_name_2]
    videos_stats = pd.DataFrame()  # Initiate blank data frame to hold the summary stats

    for video_idx in range(len(videos)):
        video = videos[video_idx]

        filtered = merged[merged['video_id'] == video]
        mean, sd = compute_speaker_pitch_summary(dfr=filtered)

        # Make a one-row dataframe to be appended
        video_stat = pd.DataFrame({'video': video,
                                   'mean': mean,
                                   'sd': sd}, index=[0])

        # Append the one-row dataframe
        videos_stats = pd.concat([videos_stats, video_stat])

    # Since all rows have index 0, reset index gives it running index.
    videos_stats.reset_index(inplace=True, drop=True)

    return videos_stats


def annotate_pitch(video_name_1, video_name_2, parallel_run_settings):
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        "Stage_2",
                                        'weaved talkturns.csv'))

    videos_stats = compute_coversation_pitch_summary(video_name_1,
                                                     video_name_2,
                                                     parallel_run_settings)

    # PAUSE, I need to visualize the distribution of pitch for all speakers to decide my z-score rule


def extract_delay(video_name_1, video_name_2, parallel_run_settings):
    '''
    Computes for the talkturn delay
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        "Stage_2",
                                        'weaved talkturns.csv'))
    talkturn['previous_speaker'] = talkturn['speaker'].shift()
    talkturn['next_start_time'] = talkturn['start time'].shift(-1)
    talkturn['delay'] = talkturn['next_start_time'] - talkturn['end time']
    talkturn['speaker_match'] = np.where((talkturn['speaker'] != talkturn['previous_speaker']),
                                         0, 1)
    idx = np.where((talkturn['speaker_match'] == 0) & (talkturn['delay'] > 0))
    final_talkturn = talkturn.loc[idx]
    final_talkturn.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                       video_name_1 + '_' + video_name_2,
                                       "Stage_2",
                                       'talkturn_delay.csv'),
                          index=False)


if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('joshua_linux')

    video_name_1 = 'Ses01F_F'
    video_name_2 = 'Ses01F_M'
