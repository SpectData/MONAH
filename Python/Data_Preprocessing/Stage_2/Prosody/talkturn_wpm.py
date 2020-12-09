'''
This script computes for words per minute per talkturn of each video and speaker
'''
import os
import pandas as pd
import data.secrets.parallel_run_settings_secret as prs

def extract_speech_rate(video_name_1, video_name_2):
    '''
    Computes for speech rate per talkturn
    :return: none
    '''
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")

    # Load dataframes
    talkturn = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                        video_name_1 + '_' + video_name_2,
                                        "Stage_2",
                                        "weaved talkturns.csv"))
    talkturn['wordcount'] = talkturn['text'].str.split().str.len()
    talkturn['duration'] = talkturn.apply(lambda x: x['end time'] - x['start time'], axis=1)
    talkturn['duration'] = talkturn.duration.apply(lambda x: 1 if x == 0 else x)
    talkturn['wpm'] = talkturn.apply(lambda x: x['wordcount'] * 60.0 / (x['duration']), axis=1)

    wpm = talkturn[['video_id', 'audio_id', 'speaker', 'talkturn no', 'wpm']]
    wpm.to_csv(os.path.join(parallel_run_settings['csv_path'],
                            video_name_1 + '_' + video_name_2,
                            "Stage_2",
                            "talkturn_wpm.csv"), index=False)

if __name__ == '__main__':
    extract_speech_rate(video_name_1='Ses01F_F', video_name_2='Ses01F_M')
