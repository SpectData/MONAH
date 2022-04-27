'''
This script tests the accurateness of the main script results
'''
import pandas as pd
from datetime import datetime
import Python.Data_Preprocessing.Stage_3.main as m3
import Python.Data_Preprocessing.Stage_4.main as m4
import Python.Data_Preprocessing.config.dir_config as prs
import Python.Data_Preprocessing.config.sql_connection as sc

if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    video_list = pd.DataFrame({'video_1': ['Ses03M_impro07_M'],'video_2': ['Ses03M_impro07_F']})
    #video_list = pd.DataFrame({'video_1': ['Ses01F_impro01_F', 'Ses01F_impro02_F', 'Ses01F_impro03_F', 'Ses01F_impro04_F', 'Ses01F_impro05_F'],
    #                           'video_2': ['Ses01F_impro01_M', 'Ses01F_impro02_M', 'Ses01F_impro03_M', 'Ses01F_impro04_M', 'Ses01F_impro05_M']})
    for i in range(len(video_list)):
        video_1 = video_list.video_1[i]
        video_2 = video_list.video_2[i]
        start = datetime.now()
        m3.weave_vpa(video_1=video_1, video_2=video_2,
                  delay=1, tone=1, speech_rate=1,
                  au_action=1, posiface=1, smile=1, headnod=1, leanforward=1,
                  parallel_run_settings=parallel_run_settings)
        m4.weave_session_level_transcript(video_name_1=video_1,
                                       video_name_2=video_2,
                                       d_word_count=1,
                                       p_delay=1,
                                       p_wpm=1,
                                       p_tone=1,
                                       a_au=1,
                                       a_posiface=1,
                                       a_smile=1,
                                       parallel_run_settings=parallel_run_settings
                                       )
        print('All Stages Run Time: ', datetime.now() - start)
        print('Done data processing for ' + video_1 + ' and ' + video_2, '\n')



