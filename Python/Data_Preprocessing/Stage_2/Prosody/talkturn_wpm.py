'''
This script computes for words per minute per talkturn of each video and speaker
'''
import os
import pathlib
from datetime import datetime
import pandas as pd


def extract_speech_rate(video_1, video_2, parallel_run_settings):
    '''
    Computes for speech rate per talkturn
    :return: none
    '''
    # parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    # Mark - add a condition that stops the function from running again if file exists
    if os.path.exists(str(pathlib.Path(os.path.join(parallel_run_settings['csv_path'], video_1 + '_' + video_2, 'Stage_2', 'talkturn_wpm.csv')))):
        return print('Stage 2 Prosody - WPM Exists')

    start = datetime.now()
    # Load dataframes
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_1 + '_' + video_2,
                                        "Stage_2",
                                        "weaved talkturns.csv"))
    talkturn['wordcount'] = talkturn['text'].str.split().str.len()
    talkturn['duration'] = talkturn.apply(lambda x: x['end time'] - x['start time'], axis=1)
    talkturn['duration'] = talkturn.duration.apply(lambda x: 1 if x == 0 else x)
    talkturn['wpm'] = talkturn.apply(lambda x: x['wordcount'] * 60.0 / (x['duration']), axis=1)

    wpm = talkturn[['video_id', 'audio_id', 'speaker', 'talkturn no', 'wpm']]
    wpm.to_csv(os.path.join(parallel_run_settings['csv_path'],
                            video_1 + '_' + video_2,
                            "Stage_2",
                            "talkturn_wpm.csv"), index=False)
    print('Stage 2 Prosody SpeechRate Time: ', datetime.now() - start)

if __name__ == '__main__':
    extract_speech_rate(video_1='Ses01F_F', video_2='Ses01F_M')
