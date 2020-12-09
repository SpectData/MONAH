import parselmouth

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

sns.set() # Use seaborn's default style to make attractive graphs

# Plot nice figures using Python's "standard" matplotlib library
snd = parselmouth.Sound("Examples/the_north_wind_and_the_sun.wav")

pitch = snd.to_pitch()

pitch_values = pitch.selected_array['frequency']
pitch_values[pitch_values==0] = np.nan

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