# This is the controller of talkturn_pitch_vol


import os

import Python.Data_Preprocessing.Stage_2.Prosody.word_pitch_vol as wpv
import Python.Data_Preprocessing.Stage_2.Verbatim.execute_weaving_talkturn as wvt
import Python.Data_Preprocessing.config.dir_config as prs


def create_talkturn_pitch_vol(video_name_1, video_name_2, parallel_run_settings, require_pitch_vol):
    '''
    (1) Create word_pitch_vol whether or not require_pitch_vol is true or false
    (2) Call execute weave talkturn after (1)
    :param video_name_1:
    :param video_name_2:
    :param parallel_run_settings:
    :param require_pitch_vol:
    :return:
    '''

    # Create the word pitch and volume
    wpv.create_pitchvol_words(require_pitch_vol)

    # Reweave the talkturns using words with annotations
    wvt.run_weaving_talkturn(video_name_1, video_name_2, parallel_run_settings,
                             input_filepath=os.path.join(parallel_run_settings['csv_path'],
                                                         video_name_1 + '_' + video_name_2,
                                                         'Stage_2',
                                                         "pitch_vol_words.csv"),
                             output_filepath=os.path.join(parallel_run_settings['csv_path'],
                                                          video_name_1 + '_' + video_name_2,
                                                          'Stage_2',
                                                          'talkturn_pitch_vol.csv'))


if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('joshua_linux')

    video_name_1 = 'Ses01F_F'
    video_name_2 = 'Ses01F_M'
