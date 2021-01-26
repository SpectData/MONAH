'''
This script computes for pitch per talkturn
This should be in stage 2
Because we need:
(1) the entire speaker wav separated file
(2) the word level timing
(3) after the talkturn transcript so that we can get the speaker talkturns and concat them together
to compute sd and mean
'''
import glob
import os

import numpy as np
import pandas as pd
import parselmouth

import Python.Data_Preprocessing.config.dir_config as prs


def cartesian_product_basic(left, right):
    '''
    Perfroms basic cross join
    https://stackoverflow.com/questions/53699012/performant-cartesian-product-cross-join-with-pandas
    :param left:
    :param right:
    :return:
    '''
    return (left.assign(key=1).merge(right.assign(key=1), on='key').drop('key', 1))


def get_talkturn_and_wav(video_name_1, video_name_2, parallel_run_settings):
    '''
    Get dataframe containing the talkturn transcripts and wav paths

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

    return merged


def compute_speaker_pitch_summary(dfr):
    '''
    Compute mean and sd of pitch given the dataframe of wav paths
    :param dfr:
    :return:
    '''
    speaker_pitch = pd.DataFrame()
    talkturn_idx = 0
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
        pitch_df['video_id'] = talkturn_i['video_id']

        # Drop silences that will affect sd and mean calculation
        pitch_df = pitch_df[pitch_df['F0'] > 0]

        if len(pitch_df) > 0:
            speaker_pitch = pd.concat([speaker_pitch, pitch_df], axis=0)

    mean = speaker_pitch['F0'].mean()
    sd = speaker_pitch['F0'].std()

    return mean, sd, speaker_pitch


def compute_coversation_pitch_summary(video_name_1, video_name_2, parallel_run_settings):
    '''
    We need the speaker-level pitch summary to understand when is considered high pitch
    for that speaker. Specifically, we need the mean and sd of pitch for that speaker.
    :param video_name_1:
    :param video_name_2:
    :param parallel_run_settings:
    :return:
    '''

    merged = get_talkturn_and_wav(video_name_1, video_name_2, parallel_run_settings)

    # For each speaker, calculate the sd and mean of the pitch
    videos = [video_name_1, video_name_2]
    videos_stats = pd.DataFrame()  # Initiate blank data frame to hold the summary stats
    videos_details = pd.DataFrame()
    video_idx = 0
    for video_idx in range(len(videos)):
        video = videos[video_idx]

        filtered = merged[merged['video_id'] == video]
        mean, sd, video_detail = compute_speaker_pitch_summary(dfr=filtered)

        # Make a one-row dataframe to be appended
        video_stat = pd.DataFrame({'video_id': video,
                                   'mean_pitch': mean,
                                   'sd_pitch': sd}, index=[0])

        # Append the one-row dataframe
        videos_stats = pd.concat([videos_stats, video_stat])
        videos_details = pd.concat([videos_details, video_detail])

    # Since all rows have index 0, reset index gives it running index.
    videos_stats.reset_index(inplace=True, drop=True)

    return videos_stats, videos_details


def annotate_pitch(video_name_1, video_name_2, parallel_run_settings):
    # Get mean and sd by speakers
    videos_stats, videos_details = compute_coversation_pitch_summary(video_name_1,
                                                                     video_name_2,
                                                                     parallel_run_settings)

    # Get word level timings with pitch information
    word_timing = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                           video_name_1 + '_' + video_name_2,
                                           "Stage_1",
                                           'word_transcripts.csv'))

    talkturn_wav = get_talkturn_and_wav(video_name_1, video_name_2, parallel_run_settings)

    talkturn_wav.columns = ['video_id', 'audio_id', 'speaker', 'talkturn_no', 'text',
                            'talkturn_start', 'talkturn_end', 'wav_name', 'wav_path']

    # Cross join and filter
    cross_join = cartesian_product_basic(talkturn_wav, word_timing)
    cross_join = cross_join[cross_join['video_id'] == cross_join['Audio_ID']]

    cross_join['word_in_text'] = cross_join.apply(lambda x: x.word in x.text,
                                                  axis=1)

    cross_join['wordtime_in_talkturntime'] = cross_join.apply(
        lambda x: x.start_time >= x.talkturn_start and x.start_time <= x.talkturn_end
        , axis=1)

    cross_join = cross_join[(cross_join['word_in_text']) * (cross_join['wordtime_in_talkturntime'])]

    # Quality Check - to move to unit test
    qcdf = cross_join.groupby(['talkturn_no', 'text'])['text'].count()
    qcdf = qcdf.reset_index(name='nrow')
    qcdf['text_count'] = qcdf.apply(lambda x: len(x.text.split()), axis=1)
    assert np.average(qcdf['text_count'] == qcdf['nrow']) == 1.0

    # Get pitch information
    word_pitches = cartesian_product_basic(cross_join, videos_details)
    word_pitches.columns
    word_pitches = word_pitches[word_pitches['video_id_x'] == word_pitches['video_id_y']]

    word_pitches['approximate_time_match'] = word_pitches.apply(
        lambda x: x.time >= x.start_time - 0.05 and x.time <= x.start_time + 0.05
        , axis=1)

    word_pitches = word_pitches[word_pitches['approximate_time_match']]

    # Once approximately time matched, compute average across time matches
    # Intuition: computing the average F0 across +/- 0.05 seconds

    word_stats = word_pitches.groupby(['video_id', 'talkturn_no', 'text', 'word'])[
        'start_time', 'F0'].mean()
    word_stats = word_stats.reset_index()
    word_stats = word_stats.sort_values(['video_id', 'talkturn_no', 'start_time'])

    word_stats = pd.merge(word_stats, videos_stats)
    word_stats['z_pitch'] = (word_stats['F0'] - word_stats['mean_pitch']) / \
                            word_stats['sd_pitch']

    # I have visualized the distribution of pitches, mostly unimodal
    up_arrow = u"\u2191"
    print(up_arrow + 'hi')


if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('joshua_linux')

    video_name_1 = 'Ses01F_F'
    video_name_2 = 'Ses01F_M'
