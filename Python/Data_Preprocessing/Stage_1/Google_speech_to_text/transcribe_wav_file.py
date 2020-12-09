'''
This script uses Google cloud storage to transcribe wav file
'''

import os
import pandas as pd
from google.cloud import speech_v1p1beta1 as speech

def transcribe_gcs(bucket_name, audio_id):
    '''
    This function asynchronously transcribes the audio file.
    :param bucket_name: bucket_name in google cloud
    :param audio_id: wav file
    :return: word and utterance transcripts
    '''

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./data/secrets/69b431b78607.json"
    client = speech.SpeechClient()
    gcs_uri = "gs://" + bucket_name + "/" + audio_id
    audio = speech.types.RecognitionAudio(uri=gcs_uri)
    config = speech.types.RecognitionConfig(
        encoding=speech.enums.RecognitionConfig.AudioEncoding.LINEAR16,
        sample_rate_hertz=32000,
        language_code='en-US',
        enable_speaker_diarization=True,
        diarization_speaker_count=1,
        audio_channel_count=1,
        enable_separate_recognition_per_channel=True)

    operation = client.long_running_recognize(config, audio)
    print('\nWaiting for operation to complete...')
    response = operation.result(timeout=10000)
    print('\nCompleted!')

    row_list = []
    sub_row_list = []
    for result in response.results:
        row_i = {'transcript': result.alternatives[0].transcript,
                 'confidence': result.alternatives[0].confidence}
        for alternative in result.alternatives:
            for word_info in alternative.words:
                sub_row_i = {'word': word_info.word,
                             'start_time': (word_info.start_time.seconds +
                                            word_info.start_time.nanos*1e-9),
                             'speaker_tag': word_info.speaker_tag}
                sub_row_list.append(sub_row_i)
        row_list.append(row_i)

    df_word = pd.DataFrame(sub_row_list)
    df_word['Audio_ID'] = audio_id[:-4]
    cols_word = df_word.columns.tolist()
    cols_word = cols_word[-1:] + cols_word[:-1]
    df_word = df_word[cols_word]
    df_word.drop_duplicates(inplace=True)

    df_talkturn = pd.DataFrame(row_list)
    df_talkturn['Audio_ID'] = audio_id[:-4]
    cols_talkturn = df_talkturn.columns.tolist()
    cols_talkturn = cols_talkturn[-1:] + cols_talkturn[:-1]
    df_talkturn = df_talkturn[cols_talkturn]
    df_talkturn.drop_duplicates(inplace=True)

    return df_talkturn, df_word

if __name__ == '__main__':
    RESULT = transcribe_gcs(bucket_name="marriane-bucket",
                            audio_id="operator_poppy.wav")
    print(RESULT[0])
    print(RESULT[1])
