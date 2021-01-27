'''
Main manager for formulating vpa text_blob
'''

from datetime import datetime

import Python.Data_Preprocessing.Stage_1.Audio_files_manipulation.copy_mp4_files as cmf
import Python.Data_Preprocessing.Stage_2.Actions.talkturn_family_actions as tfa
import Python.Data_Preprocessing.Stage_2.Prosody.talkturn_family_prosody as tfp
import Python.Data_Preprocessing.Stage_3.narrative_fine as atb
import Python.Data_Preprocessing.config.dir_config as prs


class run_settings():
    prs = {}
    prs['OpenFace_CSV_Path'] = str('hihi')

    prs['video_1'] = None
    prs['video_2'] = None


def set_video(set_key, set_val):
    p1 = run_settings()
    p1.prs[set_key] = set_val
    print(p1.prs)


def weave_vpa(video_1, video_2, delay, tone, speech_rate, au_action, posiface, smile,
              headnod, leanforward, parallel_run_settings):
    '''
    Weave text blob for vpa family
    :param video_name_1: name of first video file
    :param video_name_2: name of second video file
    :return: none
    '''
    overall_start = datetime.now()
    start = datetime.now()

    # Stage 1 runs - transcripts
    cmf.run_creating_directories(video_1, video_2, parallel_run_settings)
    # TODO: write if statements to detect if we can skip some steps
    # exa.run_extracting_audio(parallel_run_settings)
    # gst.run_google_speech_to_text(video_1, video_2, parallel_run_settings)
    opf.run_open_face(video_1, video_2, parallel_run_settings)
    wvt.run_weaving_talkturn(video_1, video_2, parallel_run_settings,
                             input_filepath=os.path.join(parallel_run_settings['csv_path'],
                                                         video_name_1 + '_' + video_name_2,
                                                         'Stage_1',
                                                         "word_transcripts.csv"),
                             output_filepath=os.path.join(parallel_run_settings['csv_path'],
                                                          video_name_1 + '_' + video_name_2,
                                                          'Stage_2',
                                                          'weaved talkturns.csv'))
    exv.run_vokaturi(video_1, video_2, parallel_run_settings)
    print("Done data processing - Stage 1")

    print('Stage 1 Time: ', datetime.now() - start)
    start = datetime.now()

    # Stage 2 runs - processed tables
    tfp.combine_prosody_features(video_1, video_2, parallel_run_settings)
    tfa.combine_actions_features(video_1, video_2, parallel_run_settings)
    print("Done data processing - Stage 2")

    print('Stage 2 Time: ', datetime.now() - start)
    start = datetime.now()

    # Stage 3 - runs - text blob narratives
    atb.weave_narrative(video_1, video_2,
                        delay, tone, speech_rate,
                        au_action, posiface, smile,
                        headnod, leanforward,
                        parallel_run_settings)
    print("Done data processing - Stage 3")

    print('Stage 3 Time: ', datetime.now() - start)
    print('All Stages Run Time: ', datetime.now() - overall_start)


if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('joshua_linux')

    weave_vpa(video_1='Ses01F_F',
              video_2='Ses01F_M',
              delay=1,
              tone=1,
              speech_rate=1,
              au_action=1,
              posiface=1,
              smile=1,
              headnod=1,
              leanforward=1,
              parallel_run_settings=parallel_run_settings)
