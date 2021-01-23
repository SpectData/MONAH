import glob
import logging
import math
import os
from textwrap import wrap

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import parselmouth

import Python.Data_Preprocessing.config.dir_config as dcf


def _get_wav_df(parallel_run_settings):
    '''
    Get the keys and paths of waves per talkturn
    Those one-word talkturns don't have a wav file because
    they have only a start time and not a end time
    :param parallel_run_settings:
    :return: wav_df
    '''
    # List the talkturn wavs that have been cut
    wav_files = glob.glob(parallel_run_settings['talkturn_wav_path'] + "/*.wav")
    wav_df = pd.DataFrame(wav_files, columns=['path'])

    wav_df['video_id'] = 'unk'
    wav_df['audio_id'] = 'unk'
    wav_df['talkturn_id'] = -1

    for idx in range(len(wav_df)):
        wav_i = wav_df['path'][idx]
        wav_i = os.path.basename(wav_i)
        wav_i = os.path.splitext(wav_i)[0]

        video_id = wav_i[:wav_i.rfind('_')]
        talkturn_id = int(wav_i[wav_i.rfind('_') + 1:])
        audio_id = wav_i[:wav_i.find('_')]

        wav_df['video_id'].at[idx] = video_id
        wav_df['audio_id'].at[idx] = audio_id
        wav_df['talkturn_id'].at[idx] = talkturn_id

    wav_df = wav_df.sort_values(by='talkturn_id')

    return wav_df


def _get_talkturn_timings(video_name_1, video_name_2, parallel_run_settings):
    '''
    Get a dataframe that contains the path of all talkturns (if applicable)
    :param video_name_1:
    :param video_name_2:
    :param parallel_run_settings:
    :return:
    '''

    parallel_run_settings['talkturn_wav_path']

    # Narrative fine has the multimodal annotations but not the start and end time
    # Require to merge with weaved talkturns
    text_narrative = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                              video_name_1 + '_' + video_name_2,
                                              "Stage_3",
                                              "narrative_fine.csv"))

    weaved_talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                               video_name_1 + '_' + video_name_2,
                                               "Stage_2",
                                               'weaved talkturns.csv'))

    assert len(text_narrative) == len(weaved_talkturn)


    narrative_timings = text_narrative.merge(weaved_talkturn)
    narrative_timings = narrative_timings[['video_id', 'audio_id', 'talkturn no',
                                           'text_blob', 'start time', 'end time']]

    wav_df = _get_wav_df(parallel_run_settings)

    text_narrative.reset_index(drop=True, inplace=True)
    wav_df.reset_index(drop=True, inplace=True)

    pitch_df = narrative_timings.merge(wav_df,
                                       left_on=['video_id', 'audio_id', 'talkturn no'],
                                       right_on=['video_id', 'audio_id', 'talkturn_id'],
                                       how='left'
                                       )

    assert len(pitch_df) == len(narrative_timings)

    pitch_df['start_time_lead'] = pitch_df['start time'].shift(-1)

    return pitch_df


def _split_text_blob(text_blob):
    '''
    Split the MONAH text blob into the verbal and non verbal content
    :param text_blob:
    :return:
    '''
    split_point = text_blob.find('said') + len(' said')

    nonverbal = text_blob[:split_point]
    verbal = text_blob[split_point:]

    return nonverbal, verbal


def _get_word_timings(audio_id, start_time, end_time,
                      parallel_run_settings, video_name_1, video_name_2):
    '''
    References the word_transcripts.csv to get word timings
    :param audio_id:
    :param start_time:
    :param end_time:
    :param parallel_run_settings:
    :param video_name_1:
    :param video_name_2:
    :return:
    '''
    job_name = video_name_1 + '_' + video_name_2
    csv_path = os.path.join(parallel_run_settings['csv_path'], job_name, 'Stage_1')
    word_transcript = pd.read_csv(os.path.join(csv_path, 'word_transcripts.csv'))

    filtered = word_transcript[(word_transcript['Audio_ID'] == audio_id) & \
                               (word_transcript['start_time'] >= start_time) & \
                               (word_transcript['start_time'] <= end_time)]

    filtered = filtered[['Audio_ID', 'word', 'start_time']]
    filtered = filtered.drop_duplicates()

    return filtered


def _avg_soothe(word_positions):
    '''
    For word timings that have na F0, we first fill with median, then if possible, fill with average
    of the F0 of the previous and next word.
    :param word_positions:
    :return:
    '''
    idx = 1
    median = word_positions['F0'].median()
    word_positions['F0'].fillna(median, inplace=True)

    for idx in range(len(word_positions)):
        wp_i = word_positions.iloc[idx]
        if wp_i['F0'] == median and idx > 0 and idx < len(word_positions):
            word_positions['F0'].at[idx] = (word_positions['F0'].iloc[idx - 1] + \
                                            word_positions['F0'].iloc[idx + 1]) / 2

    return word_positions


def _plot_graph(text_blob, filtered_pitch_df, word_positions, export_filename):
    '''
    Takes in the word positions and pitch information and exports the pitch visualization
    :param filtered_pitch_df:
    :param word_positions:
    :param export_filename:
    :return:
    '''
    fig = plt.figure()
    ax = fig.add_subplot(111)

    fig.suptitle("\n".join(wrap(text_blob, 60)), fontsize=8)

    ax.plot(filtered_pitch_df['time'], filtered_pitch_df['F0'], 'o', markersize=2, color='cyan')
    ax.grid(False)
    plt.ylim(50,
             400)  # 125 - 300 Hz
    plt.xlim(filtered_pitch_df['time'].min() - 1.0,
             filtered_pitch_df['time'].max() + 1.0)
    plt.ylabel("fundamental frequency [Hz]")
    plt.xlabel("Time [s]")
    ax.tick_params(axis='x', colors='grey')
    ax.tick_params(axis='y', colors='grey')
    ax.xaxis.label.set_color('grey')
    ax.yaxis.label.set_color('grey')

    # Add labels to the plot
    style = dict(size=10, color='black', ha='left')
    texts = []

    for idx2 in range(len(word_positions)):
        row_j = word_positions.iloc[idx2]
        texts.append(ax.text(row_j['start_time'],
                             row_j['F0'],
                             row_j['word'], **style))

    # Helps with overlapping texts
    # texts = adjust_text(texts)

    if export_filename:
        plt.savefig(export_filename)

    plt.show()


if __name__ == '__main__':
    parallel_run_settings = dcf.get_parallel_run_settings('joshua_linux')

    video_name_1 = 'Ses01F_F'
    video_name_2 = 'Ses01F_M'
    talkturn_timings = _get_talkturn_timings(video_name_1, video_name_2, parallel_run_settings)

    # For each row of pitch df, it would be either
    # path is nan: No pitch visualization
    # path is not nan: pitch visualization

    idx = 0
    row_i = talkturn_timings.iloc[idx]
    video_id = list(set(talkturn_timings['video_id']))[0]

    logger = logging.getLogger(__name__)

    for video_id in set(talkturn_timings['video_id']):
        print(video_id)
        wav_path = os.path.join(parallel_run_settings['wav_path'], video_id + '.wav')

        audio_id = video_id

        snd = parselmouth.Sound(wav_path)
        pitch = snd.to_pitch()
        pitch_values = pitch.selected_array['frequency']
        pitch_values[pitch_values == 0] = np.nan
        pitch_df = pd.DataFrame()
        pitch_df['time'] = pitch.xs()
        pitch_df['F0'] = pitch_values

        audio_length = pitch_df['time'].max()

        talkturn_timings
        speaker_timings = talkturn_timings[talkturn_timings['video_id'] == video_id]
        idx = 10
        for idx in range(len(speaker_timings)):
            row_i = speaker_timings.iloc[idx]

            nonverbal, verbal = _split_text_blob(row_i['text_blob'])

            start_time = row_i['start time']
            end_time = row_i['start_time_lead']
            if math.isnan(end_time):
                end_time = audio_length

            filtered_pitch_df = pitch_df[(pitch_df['time'] >= start_time) & \
                                         (pitch_df['time'] < end_time)]
            try:
                filtered_pitch_df_no_na = filtered_pitch_df[filtered_pitch_df['F0'] > 0]

                word_timings = _get_word_timings(audio_id, start_time, end_time,
                                                 parallel_run_settings,
                                                 video_name_1, video_name_2)

                word_timings.columns
                filtered_pitch_df.columns
                word_positions = pd.merge_asof(word_timings, filtered_pitch_df_no_na,
                                               left_on="start_time", right_on="time",
                                               direction="nearest",
                                               tolerance=500)

                word_positions = _avg_soothe(word_positions)

                job_id = video_name_1 + '_' + video_name_2
                row_i
                export_filename = os.path.join(parallel_run_settings['csv_path'],
                                               job_id,
                                               'Stage_4',
                                               'pitch_viz')

                os.makedirs(export_filename, exist_ok=True)

                export_filename = os.path.join(export_filename,
                                               row_i['video_id'] + '_talkturn' + str(
                                                   row_i['talkturn no']) + '.jpg')

                _plot_graph(row_i['text_blob'], filtered_pitch_df, word_positions,
                            export_filename=export_filename)
            except:
                logger.info('Encountered a talkturn with problem, skip.')
