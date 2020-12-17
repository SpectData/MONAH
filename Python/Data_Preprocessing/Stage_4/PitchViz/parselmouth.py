import glob
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

import Python.Data_Preprocessing.config.dir_config as dcf
import parselmouth

sns.set()  # Use seaborn's default style to make attractive graphs

# Plot nice figures using Python's "standard" matplotlib library
snd = parselmouth.Sound("Examples/the_north_wind_and_the_sun.wav")

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

video_name_1 = 'Ses01F_F'
video_name_2 = 'Ses01F_M'

if __name__ == '__main__':
    parallel_run_settings = dcf.get_parallel_run_settings('joshua_linux')
    parallel_run_settings['talkturn_wav_path']

    text_narrative = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                              video_name_1 + '_' + video_name_2,
                                              "Stage_3",
                                              "narrative_fine.csv"))

    row_i = text_narrative.iloc[0]
    os.path.join(parallel_run_settings['talkturn_wav_path'],
                 row_i['audio_id'] + '_' + str(row_i['talkturn no']))

    # List the talkturn wavs that have been cut
    wav_files = glob.glob(parallel_run_settings['talkturn_wav_path'] + "/*.wav")
    wav_df = pd.DataFrame(wav_files, columns=['path'])
    wav_i = wav_df['path'][0]
    wav_i = os.path.basename(wav_i)
    wav_i = os.path.splitext(wav_i)[0]

    # TODO Joshua to resume here
    talkturn_id = int(wav_i[wav_i.rfind('_') + 1:])
