'''
This script combines the output of google speech to text into talkturns
'''

import gc
import os
import logging

import pandas as pd

import Python.Data_Preprocessing.config.config as cfg
import Python.Data_Preprocessing.config.dir_config as prs


def run_weaving_talkturn(video_name_1, video_name_2, parallel_run_settings,
                         input_filepath=None, output_filepath=None):
    '''
    Weave talkturns from google speech to text
    :param video_name_1:
    :param video_name_2:
    :param parallel_run_settings:
    :param input_filepath:
    :param output_filepath:
    :return:
    '''
    # gst.run_google_speech_to_text(video_name_1, video_name_2, parallel_run_settings)
    # parallel_run_settings = prs.get_parallel_run_settings("marriane_win")

    # Load dataframes
    df_word = pd.read_csv(input_filepath)
    df_word['speaker_tag'] = df_word.apply(lambda x:
                                           cfg.parameters_cfg['speaker_1']
                                           if x['Audio_ID'] == video_name_1 else
                                           cfg.parameters_cfg['speaker_2'],
                                           axis=1)
    df_word['video_id'] = df_word.Audio_ID.apply(lambda x: x)
    df_word['audio_id'] = df_word.Audio_ID.apply(lambda x: x.split("_")[0])
    df_word = df_word.drop_duplicates()
    df_word.sort_values(by=['start_time'], inplace=True)
    df_word['word number by audio'] = df_word.groupby(['audio_id']).cumcount() + 1
    df_word['speaker lagged'] = df_word['speaker_tag'].shift()
    df_word['audio lagged'] = df_word['audio_id'].shift()

    # Initialize values
    row_list = []
    k = 1

    for audio_id in list(set(df_word['audio_id'])):

        logging.getLogger().setLevel(logging.INFO)

        word_number = 1
        grouping = 0
        for word in df_word['word'][df_word['audio_id'] == audio_id]:
            video_id = (df_word['video_id'][
                (df_word['audio_id'] == audio_id) &
                (df_word['word number by audio'] == word_number)]).item()
            dfr_a = (df_word['speaker lagged'][
                (df_word['audio_id'] == audio_id) &
                (df_word['word number by audio'] == word_number)]).item()
            dfr_b = (df_word['speaker_tag'][
                (df_word['audio_id'] == audio_id) &
                (df_word['word number by audio'] == word_number)]).item()

            if ((not dfr_a) or (dfr_a == dfr_b)):
                grouping = grouping
                start_date = df_word['start_time'][
                    (df_word['audio_id'] == audio_id) &
                    (df_word['word number by audio'] == word_number)]
                speaker = dfr_b
                text = word

                row_i = {'video_id': video_id,
                         'audio_id': audio_id,
                         'talkturn no': grouping,
                         'start date': start_date.item(),
                         'speaker': speaker,
                         'text': text}
                row_list.append(row_i)

                word_number = word_number + 1

            else:
                grouping = grouping + 1
                start_date = df_word['start_time'][
                    (df_word['audio_id'] == audio_id) &
                    (df_word['word number by audio'] == word_number)]
                speaker = dfr_b
                text = word

                row_i = {'video_id': video_id,
                         'audio_id': audio_id,
                         'talkturn no': grouping,
                         'start date': start_date.item(),
                         'speaker': speaker,
                         'text': text}
                row_list.append(row_i)

                word_number = word_number + 1

        logging.info(str(k) + " Completed " + str(audio_id))

        k = k + 1

    talkturn = pd.DataFrame(row_list)
    dfr_a = talkturn.groupby(['video_id', 'audio_id', 'speaker', 'talkturn no'])['text'].apply(
        ' '.join).reset_index()
    dfr_b = talkturn.groupby(['video_id', 'audio_id', 'speaker', 'talkturn no']).agg({
        'start date': [min, max]})
    dfr_b = dfr_b.reset_index()
    dfr_b.columns = ['video_id', 'audio_id', 'speaker', 'talkturn no', 'start time', 'end time']
    talkturn = pd.merge(dfr_a, dfr_b, how='outer', on=('video_id', 'audio_id', 'speaker', 'talkturn no'))
    talkturn = talkturn.sort_values(by=['video_id', 'audio_id', 'talkturn no'],
                                    ascending=[True, True, True])
    talkturn.to_csv(output_filepath,
                    sep=',',
                    index=False,
                    encoding='utf-8')
    del [[df_word, dfr_a, dfr_b]]
    gc.collect()

if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings("marriane_linux")
    run_weaving_talkturn(video_name_1='Ses01F_impro01_F',
                         video_name_2='Ses01F_impro01_M',
                         parallel_run_settings=parallel_run_settings,
                         input_filepath=os.path.join(parallel_run_settings['csv_path'],
                                                     'Ses01F_impro01_F' + '_' + 'Ses01F_impro01_M',
                                                     'Stage_1',
                                                     "word_transcripts.csv"),
                         output_filepath=os.path.join(parallel_run_settings['csv_path'],
                                                      'Ses01F_impro01_F' + '_' + 'Ses01F_impro01_M',
                                                      'Stage_2',
                                                      'weaved talkturns.csv'))