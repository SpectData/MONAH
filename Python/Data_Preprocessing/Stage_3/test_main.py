'''
This script tests the accurateness of the main script results
'''

import os

import pandas as pd


def talkturn_lookup(talkturn_no, family):
    '''
    Look up table for expeceted text blob result
    :return:
    '''
    talkturn = {'talkturn_no': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
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
                             'the woman said well whats the problem  let me change it.'
                             ]}
    df = pd.DataFrame(data=talkturn)

    return df.loc[(df['talkturn_no'] == talkturn_no) & (df['family'] == family), 'talkturn'].values[0]

def test_blob_accurateness(video_name_1, video_name_2, parallel_run_settings):
    '''

    :return:
    '''

    # parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    actual = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                      video_name_1 + '_' + video_name_2,
                                      'Stage_3',
                                      'narrative_fine.csv'))
    matching_status = []
    for family in actual.family.unique():
        for talkturn_no in range(1,11):
            df_expected = talkturn_lookup(talkturn_no, family)
            df_actual = actual.loc[(actual['talkturn no'] == talkturn_no) &
                                   (actual['family'] == family), 'text_blob'].values[0]
            assert (df_actual == df_expected), "not matching!"
            if df_expected == df_actual:
                status = 1
            else:
                status = 0

            row_i = {'family': family,
                     'talkturn no': talkturn_no,
                     'matched': status}
            matching_status.append(row_i)

    dfr = pd.DataFrame(matching_status)

    return dfr

if __name__ == '__main__':
    result = test_blob_accurateness(video_name_1='Ses01F_F', video_name_2='Ses01F_M')
    print(result)
