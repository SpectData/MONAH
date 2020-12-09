'''
Main manager for formulating vpa text_blob
'''

import Python.Data_Preprocessing.Stage_1.Audio_files_manipulation.copy_mp4_files as cmf
import Python.Data_Preprocessing.Stage_1.FFMpeg.execute_extracting_audio as exa
import Python.Data_Preprocessing.Stage_1.Google_speech_to_text.execute_google_speech_to_text as gst
import Python.Data_Preprocessing.Stage_1.OpenFace.execute_open_face_to_dataset as opf
import Python.Data_Preprocessing.Stage_1.Vokaturi.execute_vokaturi as exv
import Python.Data_Preprocessing.Stage_2.Verbatim.execute_weaving_talkturn as wvt
import Python.Data_Preprocessing.Stage_2.Actions.talkturn_family_actions as tfa
import Python.Data_Preprocessing.Stage_2.Prosody.talkturn_family_prosody as tfp
import Python.Data_Preprocessing.Stage_3.narrative_fine as atb

class run_settings():
    prs = {}
    prs['OpenFace_CSV_Path'] = str('hihi')

    prs['video_1'] = None
    prs['video_2'] = None

def set_video(set_key, set_val):
    p1 = run_settings()
    p1.prs[set_key] = set_val
    print(p1.prs)

def weave_vpa(video_1, video_2, delay, tone, speech_rate, au_action, posiface, smile):
    '''
    Weave text blob for vpa family
    :param video_name_1: name of first video file
    :param video_name_2: name of second video file
    :return: none
    '''

    # Stage 1 runs - transcripts
    cmf.run_creating_directories(video_1, video_2)
    # exa.run_extracting_audio()
    # gst.run_google_speech_to_text(video_1, video_2)
    opf.run_open_face(video_1, video_2)
    # wvt.run_weaving_talkturn(video_1, video_2)
    exv.run_vokaturi(video_1, video_2)
    print("Done data processing - Stage 1")

    # Stage 2 runs - processed tables
    tfp.combine_prosody_features(video_1, video_2)
    tfa.combine_actions_features(video_1, video_2)
    print("Done data processing - Stage 2")

    # Stage 3 - runs - text blob narratives
    atb.weave_narrative(video_1, video_2,
                        delay, tone, speech_rate,
                        au_action, posiface, smile)
    print("Done data processing - Stage 3")

if __name__ == '__main__':
    weave_vpa(video_1='Ses01F_F',
              video_2='Ses01F_M',
              delay=1,
              tone=1,
              speech_rate=1,
              au_action=1,
              posiface=1,
              smile=1)
