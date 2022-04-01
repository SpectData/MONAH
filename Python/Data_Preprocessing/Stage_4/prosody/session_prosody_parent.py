'''
This script combines all the action session level transcript
'''

# Load libraries
import pandas as pd

pd.__version__

import os
import pathlib
from datetime import datetime
import Python.Data_Preprocessing.Stage_4.prosody.session_prosody_speechrate as sps
import Python.Data_Preprocessing.Stage_4.prosody.session_prosody_delay as spd
import Python.Data_Preprocessing.Stage_4.prosody.session_prosody_tone as spt


def get_prosody_blob(video_name_1, video_name_2, parallel_run_settings, delay, wpm, tone):
    '''
    Combining all action transcripts in one blob
    :param video_name_1:
    :param video_name_2:
    :param parallel_run_settings:
    :param delay:
    :param wpm:
    :param tone:
    :return:
    '''
    # parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    if os.path.exists(str(pathlib.Path(os.path.join(parallel_run_settings['csv_path'], video_name_1 + '_' + video_name_2, 'Stage_4', 'Prosody', 'narrative_coarse.csv')))):
        return print('Stage 4 Prosody File Exists')

    start = datetime.now()
    speechrate_blob = sps.get_all_blob(video_name_1, video_name_2, parallel_run_settings)
    delay_blob = spd.get_all_blob(video_name_1, video_name_2, parallel_run_settings)
    tone_blob = spt.get_all_blob(video_name_1, video_name_2, parallel_run_settings)

    speechrate_blob = speechrate_blob.sort_values(by=['Video_ID'])
    speechrate_blob['blob'] = speechrate_blob.blob.apply(lambda x: x if wpm == 1 else '')

    delay_blob = delay_blob.sort_values(by=['Video_ID'])
    delay_blob['blob'] = delay_blob.blob.apply(lambda x: x if delay == 1 else '')

    tone_blob = tone_blob.sort_values(by=['Video_ID'])
    tone_blob['blob'] = tone_blob.blob.apply(lambda x: x if tone == 1 else '')

    len(set(speechrate_blob.Video_ID))
    len(set(delay_blob.Video_ID))
    len(set(tone_blob.Video_ID))

    prosody_blob = pd.merge(left=tone_blob, right=delay_blob,
                            on=['Video_ID'],
                            how='left')

    prosody_blob.fillna(value='', inplace=True)

    prosody_blob['prosody_blob'] = prosody_blob['blob_x'] + \
                                   prosody_blob['blob_y']

    prosody_blob = prosody_blob[['Video_ID', 'prosody_blob']]

    prosody_blob = pd.merge(left=prosody_blob, right=speechrate_blob,
                            on=['Video_ID'],
                            how='left')

    prosody_blob.fillna(value='', inplace=True)

    prosody_blob['prosody_blob'] = prosody_blob['prosody_blob'] + \
                                   prosody_blob['blob']

    prosody_blob = prosody_blob[['Video_ID', 'prosody_blob']]

    # EXPORT

    prosody_blob['family'] = 'prosody'
    export = prosody_blob[['Video_ID', 'family', 'prosody_blob']]
    export.columns = ['Video_ID', 'family', 'text_blob']

    export.to_csv(os.path.join(parallel_run_settings['csv_path'],
                               video_name_1 + '_' + video_name_2,
                               'Stage_4',
                               'Prosody',
                               'narrative_coarse.csv'), index=False)
    print('Stage 4 Prosody Time: ', datetime.now() - start)

if __name__ == '__main__':
    get_prosody_blob(video_name_1='Ses01F_F',
                     video_name_2='Ses01F_M',
                     delay=1,
                     wpm=1,
                     tone=1)
