'''
Generates word count data by session and speaker
'''

# Load libraries
import pandas as pd
import logging
import os
import numpy as np
import data.secrets.parallel_run_settings_secret as prs

def run_dataframe(video_name_1, video_name_2):
    '''
    Load dataframe of summary metrics
    :return: dataframe of metrics
    '''
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    raw_data = pd.read_csv(os.path.join(parallel_run_settings['csv_path'],
                                    video_name_1 + '_' + video_name_2,
                                    'Stage_2',
                                    'weaved talkturns.csv'))
    data = raw_data.groupby(['video_id', 'speaker'])['text'].apply(' '.join).reset_index()

    return data

def extract_session_wordcount(data):
    '''
    Compute for session-level wordcount
    :param data: session data to be analyzed
    :return: word count
    '''
    wordcount_data = pd.DataFrame()
    # Count the total number of words
    for video_id in list(set(data['video_id'])):
        video_data = data[data['video_id'] == video_id]
        for speaker in video_data['speaker']:
            speaker_data = video_data[video_data['speaker'] == speaker]
            s = speaker_data['text'].item()
            for char in '-.,\n':
                s = s.replace(char, ' ')  # replace special characters with space
            speaker_data['word_count'] = len(s.split())
            wordcount_data = wordcount_data.append(speaker_data)

        logging.info(str(video_id) + " word count done")
    wordcount_data = wordcount_data[['video_id', 'speaker', 'word_count']]
    wordcount_data['audio_id'] = wordcount_data.video_id.apply(lambda x: x.split("_")[0])

    # Compute for proportion
    total_data = wordcount_data.groupby(['audio_id']).agg({'word_count': sum})
    total_data = total_data.reset_index()
    total_data.columns = ['audio_id', 'total_word_count']
    total_data = pd.merge(total_data, wordcount_data, how="inner", on="audio_id")
    total_data['word_countprop'] = total_data.apply(lambda x:
                                                    x['word_count']/x['total_word_count'], axis=1)
    total_data = total_data[['video_id', 'speaker', 'word_count', 'word_countprop']]

    return total_data

def extract_session_unique_wordcount(data):
    '''
    Compute for session level unique word count
    :param data:
    :return:
    '''
    wordcount_data = pd.DataFrame()
    # Count the total number of unique words
    for video_id in list(set(data['video_id'])):
        video_data = data[data['video_id'] == video_id]
        for speaker in video_data['speaker']:
            speaker_data = video_data[video_data['speaker'] == speaker]
            summary = speaker_data.text.apply(lambda x: pd.value_counts(x.split(" "))).sum(axis=0)
            summ = summary.to_frame().reset_index()
            summ.columns = ['word', 'count']
            summ['video_id'] = video_id
            summ['speaker'] = speaker
            summ = summ[summ['word'] != ""]

            wordcount_data = wordcount_data.append(summ)
            logging.info(str(video_id) + " unique word count done")

    unique_wordcount_data = wordcount_data.groupby(['video_id', 'speaker'])['word'].count()
    unique_wordcount_data = unique_wordcount_data.reset_index()
    unique_wordcount_data.columns = ['video_id', 'speaker', 'word_uniquecount']

    return unique_wordcount_data

def run_computing_wordcount(video_name_1, video_name_2):
    '''
    Computes for word count data
    :param video_name_1: video to be analyzed
    :param video_name_2: video_to be analyzed
    :return:
    '''
    logging.getLogger().setLevel(logging.INFO)
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    data = run_dataframe(video_name_1, video_name_2)

    word_count = extract_session_wordcount(data)
    unique_word_count = extract_session_unique_wordcount(data)

    summary = pd.merge(word_count, unique_word_count, how="inner", on=['video_id', 'speaker'])
    summary.columns = ['video_id', 'speaker', 'word_count', 'word_countprop', 'word_uniquecount']
    summary.to_csv(os.path.join(parallel_run_settings['csv_path'],
                                video_name_1 + '_' + video_name_2,
                                'Stage_4',
                                'Demographics',
                                'session_level_wordcount.csv'), index=False)

# Main Logic
if __name__ == "__main__":
    run_computing_wordcount(video_name_1="Ses01F_F", video_name_2="Ses01F_M")
