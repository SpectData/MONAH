import glob
import os

import numpy as np
import pandas as pd
import parselmouth

import Python.Data_Preprocessing.config.dir_config as prs

parallel_run_settings = prs.get_parallel_run_settings('joshua_linux')

split_folder = '/mnt/S/researchdata/IEMOCAP/iemocap-split'

wav_files = glob.glob(os.path.join(split_folder, '*.wav'))
wav_files = sorted(wav_files)

wav_file = wav_files[0]

snd = parselmouth.Sound(wav_file)

intensity = snd.to_intensity()
intensity_df = pd.DataFrame({'time': np.round(intensity.xs(), 2),
                             'intensity_value': intensity.values[0]})
intensity_df = pd.DataFrame(intensity_df.groupby('time')['intensity_value'].mean())
intensity_df['time'] = intensity_df.index
intensity_df.reset_index(inplace=True, drop=True)

pitch = snd.to_pitch()
pitch_values = pitch.selected_array['frequency']

pitch_df = pd.DataFrame({'time': np.round(pitch.xs(), 2),
                         'pitch_value': pitch_values})

pitch_values[pitch_values == 0] = np.nan

pitch_df = pitch_df[pitch_df['pitch_value'] > 0]
pitch_df = pd.DataFrame(pitch_df.groupby('time')['pitch_value'].mean())
pitch_df['time'] = pitch_df.index
pitch_df.reset_index(inplace=True, drop=True)

merged = pd.merge(intensity_df, pitch_df, how='inner')
plot_df = merged[(merged['time'] >= 30) & (merged['time'] <= 40)]

import matplotlib.pyplot as plt

fig, ax1 = plt.subplots()

color = 'tab:red'
ax1.set_xlabel('time (s)')
ax1.set_ylabel('pitch_value', color=color)
ax1.plot(plot_df['time'], plot_df['pitch_value'], color=color)
ax1.tick_params(axis='y', labelcolor=color)

ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

color = 'tab:blue'
ax2.set_ylabel('intensity_value', color=color)  # we already handled the x-label with ax1
ax2.plot(plot_df['time'], plot_df['intensity_value'], color=color)
ax2.tick_params(axis='y', labelcolor=color)

fig.tight_layout()  # otherwise the right y-label is slightly clipped
plt.show()

# TODO It seems that I need to filter away intensity < 70/75 to remove the other speaker voice.
# to see if this poses a problem on sad/quiet settings
