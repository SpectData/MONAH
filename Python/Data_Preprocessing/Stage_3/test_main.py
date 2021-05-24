'''
Tests ./main.py
'''

import os
import pandas as pd
import Python.Data_Preprocessing.config.dir_config as prs
import Python.Data_Preprocessing.Stage_3.main as main


class Test_Narrative:
    '''
    Scoring Test
    '''
    computer_name = 'marriane_linux'

    parallel_run_settings = prs.get_parallel_run_settings(computer_name)
    talkturn_lookup = pd.read_csv(os.path.join(parallel_run_settings['test_file_path'], 'test.csv'))
    talkturn_lookup2 = pd.read_csv(os.path.join(parallel_run_settings['test_file_path'], 'test2.csv'))

    # Test case #1a - verbatim
    def test_v(self):
        '''
        Check that verbatim results are all matching
        :return: assertion
        '''

        parallel_run_settings = Test_Narrative.parallel_run_settings
        expected = Test_Narrative.talkturn_lookup
        main.weave_vpa(video_1='Ses01F_F',
                       video_2='Ses01F_M',
                       gstt=0,
                       delay=0,
                       tone=0,
                       speech_rate=0,
                       au_action=0,
                       posiface=0,
                       smile=0,
                       headnod=0,
                       leanforward=0,
                       parallel_run_settings=parallel_run_settings
                       )
        actual = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                          'Ses01F_F' + '_' + 'Ses01F_M',
                                          'Stage_3',
                                          'narrative_fine.csv'))
        actual = actual.head(10)

        result = True

        for index, row in actual.iterrows():
            actual_talkturn_no = row['talkturn no']
            actual_talkturn = row['text_blob']
            expected_talkturn = expected.loc[(expected['talkturn_no'] == actual_talkturn_no) &
                                             (expected['family'] == 'v'), 'talkturn'].values[0]
            if actual_talkturn != expected_talkturn:
                result = False

        assert result

    # Test case #1b - vp
    def test_vp(self):
        '''
        Check that vp results are all matching
        :return: assertion
        '''

        parallel_run_settings = Test_Narrative.parallel_run_settings
        expected = Test_Narrative.talkturn_lookup
        main.weave_vpa(video_1='Ses01F_F',
                       video_2='Ses01F_M',
                       gstt=0,
                       delay=1,
                       tone=1,
                       speech_rate=1,
                       au_action=0,
                       posiface=0,
                       smile=0,
                       headnod=0,
                       leanforward=0,
                       parallel_run_settings=parallel_run_settings
                       )
        actual = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                          'Ses01F_F' + '_' + 'Ses01F_M',
                                          'Stage_3',
                                          'narrative_fine.csv'))
        actual = actual.head(10)

        result = True

        for index, row in actual.iterrows():
            actual_talkturn_no = row['talkturn no']
            actual_talkturn = row['text_blob']
            expected_talkturn = expected.loc[(expected['talkturn_no'] == actual_talkturn_no) &
                                             (expected['family'] == 'vp'), 'talkturn'].values[0]
            if actual_talkturn != expected_talkturn:
                result = False

        assert result

    # Test case #1c - vpa
    def test_vpa(self):
        '''
        Check that vpa results are all matching
        :return: assertion
        '''

        parallel_run_settings = Test_Narrative.parallel_run_settings
        expected = Test_Narrative.talkturn_lookup
        main.weave_vpa(video_1='Ses01F_F',
                       video_2='Ses01F_M',
                       gstt=0,
                       delay=1,
                       tone=1,
                       speech_rate=1,
                       au_action=1,
                       posiface=1,
                       smile=1,
                       headnod=1,
                       leanforward=1,
                       parallel_run_settings=parallel_run_settings
                       )
        actual = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                          'Ses01F_F' + '_' + 'Ses01F_M',
                                          'Stage_3',
                                          'narrative_fine.csv'))
        actual = actual.head(10)

        result = True

        for index, row in actual.iterrows():
            actual_talkturn_no = row['talkturn no']
            actual_talkturn = row['text_blob']
            expected_talkturn = expected.loc[(expected['talkturn_no'] == actual_talkturn_no) &
                                             (expected['family'] == 'vpa'), 'talkturn'].values[0]
            if actual_talkturn != expected_talkturn:
                result = False

        assert result

    # Test case #2 - wo BYO
    def test_v2(self):
        '''
        Check that verbatim results are all matching
        :return: assertion
        '''

        parallel_run_settings = Test_Narrative.parallel_run_settings
        # expected = Test_Narrative.talkturn_lookup2
        expected = ['forms', 'problem', 'standing', 'beginning',
                    'directed', 'filling out', 'particular form']
        main.weave_vpa(video_1='Ses01F_F',
                       video_2='Ses01F_M',
                       gstt=1,
                       delay=0,
                       tone=0,
                       speech_rate=0,
                       au_action=0,
                       posiface=0,
                       smile=0,
                       headnod=0,
                       leanforward=0,
                       parallel_run_settings=parallel_run_settings
                       )
        actual = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                          'Ses01F_F' + '_' + 'Ses01F_M',
                                          'Stage_3',
                                          'narrative_fine_gstt.csv'))
        actual = actual.head(10)

        result = True

        for keyword in expected:
            if actual.text_blob.str.contains(keyword, case = False):
                result = True
            else:
                result = False

        assert result
