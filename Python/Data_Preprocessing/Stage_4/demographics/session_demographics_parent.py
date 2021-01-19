'''
This script combines all the action session level transcript
'''

# Load libraries

import os

import Python.Data_Preprocessing.Stage_4.demographics.session_demographics_talkativeness as sdt


def get_demographics_blob(video_name_1, video_name_2, word_count, parallel_run_settings):
    '''
    Combining all action transcripts in one blob
    :param video_name_1:
    :param video_name_2:
    :return:
    '''
    # parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    word_count_blob = sdt.get_all_blob(video_name_1,
                                       video_name_2,
                                       parallel_run_settings)

    word_count_blob = word_count_blob.sort_values(by=['Video_ID'])
    word_count_blob['blob'] = word_count_blob.blob.apply(lambda x: x if word_count == 1 else '')

    len(set(word_count_blob.Video_ID))

    demographics_blob = word_count_blob
    demographics_blob.columns = ['Video_ID', 'demographics_blob']

    # EXPORT
    demographics_blob['family'] = 'demographics'
    export = demographics_blob[['Video_ID', 'family', 'demographics_blob']]
    export.columns = ['Video_ID', 'family', 'text_blob']

    export.to_csv(os.path.join(parallel_run_settings['csv_path'],
                               video_name_1 + '_' + video_name_2,
                               'Stage_4',
                               'Demographics',
                               'narrative_coarse.csv'), index=False)

if __name__ == '__main__':
    get_demographics_blob(video_name_1='Ses01F_F',
                          video_name_2='Ses01F_M',
                          word_count=1)
