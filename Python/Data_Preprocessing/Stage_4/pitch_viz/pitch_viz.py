import glob
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

import Python.Data_Preprocessing.config.dir_config as dcf


def adhoc():
    sns.set()  # Use seaborn's default style to make attractive graphs

    # Plot nice figures using Python's "standard" matplotlib library
    snd = pitch_viz.Sound("Examples/the_north_wind_and_the_sun.wav")

    pitch = snd.to_pitch()

    pitch_values = pitch.selected_array['frequency']
    pitch_values[pitch_values == 0] = np.nan

    #################
    # Matplotlib
    #################
    plt.rcParams["figure.facecolor"] = "w"

    fig = plt.figure()
    ax = fig.add_subplot(111)

    ax.plot(pitch.xs(), pitch_values, 'o', markersize=2, color='cyan')
    ax.grid(False)
    plt.ylim(0, pitch.ceiling)
    plt.ylabel("fundamental frequency [Hz]")
    plt.xlabel("Time [s]")
    ax.tick_params(axis='x', colors='grey')
    ax.tick_params(axis='y', colors='grey')
    ax.xaxis.label.set_color('grey')
    ax.yaxis.label.set_color('grey')

    # Add labels to the plot
    style = dict(size=10, color='black')

    ax.text(0.1, 50, "The", **style)
    ax.text(0.22, 50, "north", **style)
    ax.text(0.475, 50, "wind", **style)
    ax.text(0.7, 50, "and", **style)
    ax.text(0.8, 50, "the", **style)
    ax.text(1.04, 50, "Sun", **style)

    plt.show()

    # TODO
    # Ask Marriane to push dev branch
    # Current plan is to generate png at talkturn level
    # Play button simply plays two videos, doesn't highlight
    # (or simply have a roving asterisks in a table of talkturns)
    # I will coauthor with Marriane wrt to config file. Option to generate pitch visualization

    #################
    # Seaborn
    #################

    style = dict(size=10, color='black')
    sns.set(rc={'axes.facecolor': 'white', 'figure.facecolor': 'white'})

    fig, axs = plt.subplots(nrows=2)
    g1 = sns.scatterplot(data=pitch_values, ax=axs[0])
    g2 = sns.scatterplot(data=pitch_values, ax=axs[1])
    axs[0].text(0.1, 50, "?The", **style)
    plt.show()


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


def _get_pitch_df(video_name_1, video_name_2, parallel_run_settings):
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

    text_narrative.columns
    weaved_talkturn.columns

    narrative_timings = text_narrative.merge(weaved_talkturn)
    narrative_timings = narrative_timings[['video_id', 'audio_id', 'talkturn no',
                                           'text_blob', 'start time', 'end time']]

    wav_df = _get_wav_df(parallel_run_settings)

    text_narrative.reset_index(drop=True, inplace=True)
    wav_df.reset_index(drop=True, inplace=True)

    narrative_timings.columns
    wav_df.columns
    pitch_df = narrative_timings.merge(wav_df,
                                       left_on=['video_id', 'audio_id', 'talkturn no'],
                                       right_on=['video_id', 'audio_id', 'talkturn_id'],
                                       how='left'
                                       )

    assert len(pitch_df) == len(narrative_timings)

    return pitch_df


def _get_word_timings(audio_id, talkturn_no):
    pass


if __name__ == '__main__':
    parallel_run_settings = dcf.get_parallel_run_settings('joshua_linux')

    video_name_1 = 'Ses01F_F'
    video_name_2 = 'Ses01F_M'

    pitch_df = _get_pitch_df(video_name_1, video_name_2, parallel_run_settings)

    # For each row of pitch df, it would be either
    # path is nan: No pitch visualization
    # path is not nan: pitch visualization
    # TODO: Josh to resume here, start if pitch visualization, that way
    # it is easilier to just leave the pitch viz panel black when path is nan.
    idx = 0
    row_i = pitch_df.iloc[idx]
    row_i
