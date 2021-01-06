'''
Tests ./main.py
'''

import Python.Data_Preprocessing.config.dir_config as prs
import Python.Data_Preprocessing.Stage_3.main as main


class Test_Narrative:
    '''
    Scoring Test
    '''
    computer_name = 'marriane_win'

    parallel_run_settings = prs.get_parallel_run_settings(computer_name)
    talkturn_lookup = {'talkturn_no': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                       1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                       1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                       'family': ['v', 'v', 'v', 'v', 'v', 'v', 'v', 'v', 'v', 'v',
                                  'vp', 'vp', 'vp', 'vp', 'vp', 'vp', 'vp', 'vp', 'vp', 'vp',
                                  'vpa', 'vpa', 'vpa', 'vpa', 'vpa', 'vpa', 'vpa', 'vpa', 'vpa', 'vpa'],
                       'talkturn': ['the woman said excuse me.',
                                    'the man said do you have your forms.',
                                    'the woman said yeah.',
                                    'the man said let me see them.',
                                    'the woman said is there a problem.',
                                    'the man said who told you to get in this line.',
                                    'the woman said you did.',
                                    'the woman said you were standing at the beginning and you directed me.',
                                    'the man said okay but i didnt tell you to get in this line if you are filling out this particular form.',
                                    'the woman said well whats the problem  let me change it.',
                                    'the woman said excuse me.',
                                    'the man said do you have your forms.',
                                    'the woman said yeah.',
                                    'the man said let me see them.',
                                    'after two hundred milliseconds a slightly long delay the woman said is there a problem.',
                                    'the man happily and very quickly said who told you to get in this line.',
                                    'the woman sadly and quickly said you did.',
                                    'the woman said you were standing at the beginning and you directed me.',
                                    'the man angrily and very quickly said okay but i didnt tell you to get in this line if you are filling out this particular form.',
                                    'the woman said well whats the problem  let me change it.',
                                    'the woman said excuse me.',
                                    'the man said do you have your forms.',
                                    'the woman said yeah.',
                                    'the man said let me see them.',
                                    'after two hundred milliseconds a slightly long delay the woman said is there a problem.',
                                    'the man happily and very quickly said who told you to get in this line.',
                                    'the woman sadly and quickly said you did.',
                                    'the woman said you were standing at the beginning and you directed me.',
                                    'the man angrily and very quickly said okay but i didnt tell you to get in this line if you are filling out this particular form.',
                                    'the woman said well whats the problem  let me change it.'
                                    ]
                       }

    # Test case #1 - verbatim
    def test_v(self):
        '''
        Check that verbatim results are all matching
        :return: assertion
        '''

        parallel_run_settings = Test_Narrative.parallel_run_settings
        expected = Test_Narrative.talkturn_lookup
        actual = main.weave_vpa(video_1='Ses01F_F',
                                video_2='Ses01F_M',
                                delay=0,
                                tone=0,
                                speech_rate=0,
                                au_action=0,
                                posiface=0,
                                smile=0,
                                parallel_run_settings=parallel_run_settings
                                )

        result = True

        for index, row in actual[actual['family']=='v']:
            actual_talkturn_no = row['talkturn no']
            actual_talkturn = row['text_blob']
            expected_talkturn = expected.loc[(expected['talkturn_no'] == actual_talkturn_no), 'talkturn'].values[0]
            if actual_talkturn != expected_talkturn:
                result = False

        assert result

    # Test case #2 - vp
    def test_vp(self):
        '''
        Check that verbatim results are all matching
        :return: assertion
        '''

        parallel_run_settings = Test_Narrative.parallel_run_settings
        expected = Test_Narrative.talkturn_lookup
        actual = main.weave_vpa(video_1='Ses01F_F',
                                video_2='Ses01F_M',
                                delay=1,
                                tone=1,
                                speech_rate=1,
                                au_action=0,
                                posiface=0,
                                smile=0,
                                parallel_run_settings=parallel_run_settings
                                )

        result = True

        for index, row in actual[actual['family']=='vp']:
            actual_talkturn_no = row['talkturn no']
            actual_talkturn = row['text_blob']
            expected_talkturn = expected.loc[(expected['talkturn_no'] == actual_talkturn_no), 'talkturn'].values[0]
            if actual_talkturn != expected_talkturn:
                result = False

        assert result

    # Test case #3 - vpa
    def test_vpa(self):
        '''
        Check that verbatim results are all matching
        :return: assertion
        '''

        parallel_run_settings = Test_Narrative.parallel_run_settings
        expected = Test_Narrative.talkturn_lookup
        actual = main.weave_vpa(video_1='Ses01F_F',
                                video_2='Ses01F_M',
                                delay=1,
                                tone=1,
                                speech_rate=1,
                                au_action=1,
                                posiface=1,
                                smile=1,
                                parallel_run_settings=parallel_run_settings
                                )

        result = True

        for index, row in actual[actual['family']=='vpa']:
            actual_talkturn_no = row['talkturn no']
            actual_talkturn = row['text_blob']
            expected_talkturn = expected.loc[(expected['talkturn_no'] == actual_talkturn_no), 'talkturn'].values[0]
            if actual_talkturn != expected_talkturn:
                result = False

        assert result