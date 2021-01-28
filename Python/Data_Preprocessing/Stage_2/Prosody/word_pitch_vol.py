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
from shutil import copyfile

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
    speaker_intensity = pd.DataFrame()
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

        # Intensity
        intensity = snd.to_intensity()
        intensity_values = intensity.values[0]
        intensity_values[intensity_values == 0] = np.nan
        intensity_df = pd.DataFrame()
        intensity_df['time'] = intensity.xs()
        # the time is based on time in wav (start from zero for every talkturn)
        # We add the start time of everytalk turn to compute the time in conversation.
        intensity_df['time'] += talkturn_i['start time']
        intensity_df['intensity'] = intensity_values
        intensity_df['video_id'] = talkturn_i['video_id']

        # Drop silences that will affect sd and mean calculation
        intensity_df = intensity_df[intensity_df['intensity'] > 0]

        if len(intensity_df) > 0:
            speaker_intensity = pd.concat([speaker_intensity, intensity_df], axis=0)

    stats = {}
    stats['mean_pitch'] = speaker_pitch['F0'].mean()
    stats['sd_pitch'] = speaker_pitch['F0'].std()
    stats['mean_intensity'] = speaker_intensity['intensity'].mean()
    stats['sd_intensity'] = speaker_intensity['intensity'].std()

    return stats, speaker_pitch, speaker_intensity


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
    speakers_pitch = pd.DataFrame()
    speakers_intensity = pd.DataFrame()
    video_idx = 0
    for video_idx in range(len(videos)):
        video = videos[video_idx]

        filtered = merged[merged['video_id'] == video]
        stats, speaker_pitch, speaker_intensity = compute_speaker_pitch_summary(dfr=filtered)

        # Make a one-row dataframe to be appended
        video_stat = pd.DataFrame({'video_id': video,
                                   'mean_pitch': stats['mean_pitch'],
                                   'sd_pitch': stats['sd_pitch'],
                                   'mean_intensity': stats['mean_intensity'],
                                   'sd_intensity': stats['sd_intensity']
                                   }, index=[0])

        # Append the one-row dataframe
        videos_stats = pd.concat([videos_stats, video_stat])
        speakers_pitch = pd.concat([speakers_pitch, speaker_pitch])
        speakers_intensity = pd.concat([speakers_intensity, speaker_intensity])

    # Since all rows have index 0, reset index gives it running index.
    videos_stats.reset_index(inplace=True, drop=True)

    return videos_stats, speakers_pitch, speakers_intensity


def annotate_pitch(video_name_1, video_name_2, parallel_run_settings):

    # Get mean and sd by speakers
    videos_stats, speakers_pitch, speakers_intensity = compute_coversation_pitch_summary(
        video_name_1,
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
    # Asserting 100% match between number of rows and word count from text
    assert np.average(qcdf['text_count'] == qcdf['nrow']) == 1.0

    # Get pitch information
    word_pitches = cartesian_product_basic(cross_join, speakers_pitch)
    word_pitches = word_pitches[word_pitches['video_id_x'] == word_pitches['video_id_y']]

    word_pitches['approximate_time_match'] = word_pitches.apply(
        lambda x: x.time >= x.start_time - 0.05 and x.time <= x.start_time + 0.05
        , axis=1)

    word_pitches = word_pitches[word_pitches['approximate_time_match']]
    word_pitches.columns = ['video_id', 'audio_id', 'speaker', 'talkturn_no', 'text',
                            'talkturn_start', 'talkturn_end', 'wav_name', 'wav_path', 'Audio_ID',
                            'word', 'start_time', 'word_in_text', 'wordtime_in_talkturntime',
                            'time', 'F0', 'video_id_y', 'approximate_time_match']

    # Once approximately time matched, compute average across time matches
    # Intuition: computing the average F0 across +/- 0.05 seconds
    word_pitches = word_pitches.groupby(['video_id', 'talkturn_no', 'text', 'word'])[
        'start_time', 'F0'].mean()
    word_pitches = word_pitches.reset_index()
    word_pitches = word_pitches.sort_values(['video_id', 'talkturn_no', 'start_time'])

    # Get intensity information
    word_intensities = cartesian_product_basic(cross_join, speakers_intensity)
    word_intensities.columns
    word_intensities = word_intensities[
        word_intensities['video_id_x'] == word_intensities['video_id_y']]

    word_intensities['approximate_time_match'] = word_intensities.apply(
        lambda x: x.time >= x.start_time - 0.05 and x.time <= x.start_time + 0.05
        , axis=1)

    word_intensities = word_intensities[word_intensities['approximate_time_match']]
    word_intensities.columns = ['video_id', 'audio_id', 'speaker', 'talkturn_no', 'text',
                                'talkturn_start', 'talkturn_end', 'wav_name', 'wav_path',
                                'Audio_ID',
                                'word', 'start_time', 'word_in_text', 'wordtime_in_talkturntime',
                                'time', 'intensity', 'video_id_y', 'approximate_time_match']

    # Once approximately time matched, compute average across time matches
    # Intuition: computing the average F0 across +/- 0.05 seconds
    word_intensities = word_intensities.groupby(['video_id', 'talkturn_no', 'text', 'word'])[
        'start_time', 'intensity'].mean()
    word_intensities = word_intensities.reset_index()
    word_intensities = word_intensities.sort_values(['video_id', 'talkturn_no', 'start_time'])

    word_intensities.columns

    # Merge in video statistics to prepare for z computations
    word_stats = pd.merge(word_pitches, word_intensities, how='outer')
    word_stats = pd.merge(word_stats, videos_stats)

    # z computations
    word_stats['z_pitch'] = (word_stats['F0'] - word_stats['mean_pitch']) / \
                            word_stats['sd_pitch']

    word_stats['z_intensity'] = (word_stats['intensity'] - word_stats['mean_intensity']) / \
                                word_stats['sd_intensity']

    # Annotations
    up_arrow = u"\u2191"
    down_arrow = u"\u2193"

    word_stats['pitch_annotation'] = np.select(
        [
            word_stats['z_pitch'].between(3, 4, inclusive=False),
            word_stats['z_pitch'].ge(4),
            word_stats['z_pitch'].between(-3, -4, inclusive=False),
            word_stats['z_pitch'].le(-4),
        ],
        [
            up_arrow,
            up_arrow + up_arrow,
            down_arrow,
            down_arrow + down_arrow
        ],
        default=None
    )

    word_stats['intensity_annotation'] = np.select(
        [
            word_stats['intensity'].ge(80),
            word_stats['z_intensity'].ge(3)
        ],
        [
            True,
            True,
        ],
        default=False
    )
    return word_stats


def create_pitchvol_words(video_name_1, video_name_2, parallel_run_settings, require_pitch_vol):
    '''

    :param word_stats:
    :param require_pitch_vol: True only when pitch and/or vol is required.
    :return:
    '''

    if require_pitch_vol:
        word_stats = annotate_pitch(video_name_1, video_name_2, parallel_run_settings)

        has_annotations = word_stats[
            (word_stats['pitch_annotation'].notna()) | \
            (word_stats['intensity_annotation'])
            ]

        has_annotations = has_annotations[['video_id', 'word', 'start_time',
                                           'pitch_annotation',
                                           'intensity_annotation'
                                           ]]

        # Read in original word timings
        original = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                            video_name_1 + '_' + video_name_2,
                                            "Stage_1",
                                            'word_transcripts.csv'))

        # Rounding is necessary otherwise the columnns won't join
        original['start_time'] = original['start_time'].round(10)
        has_annotations['start_time'] = has_annotations['start_time'].round(10)

        merged = pd.merge(left=original, right=has_annotations, how='left',
                          left_on=['Audio_ID', "word", 'start_time'],
                          right_on=['video_id', "word", 'start_time'])

        merged = merged[['Audio_ID', 'word', 'start_time',
                         'pitch_annotation',
                         'intensity_annotation'
                         ]]

        # Temp column to word the new value of word
        merged['word_temp'] = merged['word']

        # Iterate through the rows to update the word_temp
        for row_idx in range(len(merged)):
            row_i = merged.iloc[row_idx]

            # Since pitch annotations are arrows in strings, check the presence of string
            if isinstance(row_i['pitch_annotation'], str):
                merged.at[row_idx, 'word_temp'] = row_i['pitch_annotation'] + row_i['word_temp']

            # Check if intensity annotaion is True.
            if row_i['intensity_annotation'] == True:
                merged.at[row_idx, 'word_temp'] = row_i['word_temp'].upper()

        # Extract and export relevant columns
        export = merged[['Audio_ID', 'word_temp', 'start_time']]
        export.columns = ['Audio_ID', 'word', 'start_time']

        parallel_run_settings
        export.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                   video_name_1 + '_' + video_name_2,
                                   'Stage_2',
                                   'pitch_vol_words.csv'),
                      sep=',',
                      index=False,
                      encoding='utf-8')


    else:  # require_pitch_vol == False
        # Since pitch nor volume is both not required, simply copy the word
        # timings across.
        copyfile(src=os.path.join(parallel_run_settings['csv_path'],
                                  video_name_1 + '_' + video_name_2,
                                  'Stage_1',
                                  'word_transcripts.csv'),
                 dst=os.path.join(parallel_run_settings['csv_path'],
                                  video_name_1 + '_' + video_name_2,
                                  'Stage_2',
                                  'pitch_vol_words.csv'))


if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('joshua_linux')

    video_name_1 = 'Ses01F_F'
    video_name_2 = 'Ses01F_M'
