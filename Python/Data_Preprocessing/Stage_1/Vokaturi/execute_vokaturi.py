'''
This script extracts the vokaturi features of the audio file
'''
import glob
import os
import sys
import pathlib

import pandas as pd
import scipy.io.wavfile
from tqdm import tqdm

import Python.Data_Preprocessing.Stage_1.Vokaturi.cut_wav_file as cwf
import Python.Data_Preprocessing.config.config as cfg


def insert_df(emotions_df, dest_dir):
    '''
    insert result vokaturi features to the vokaturi table
    :param emotions_df: result vokaturi features
    :return: none
    '''
    emotions_df.to_csv(os.path.join(dest_dir, "talkturn_vokaturi.csv"),
                       index=False)

def analyze_wav(wav_path, parallel_run_settings):
    '''
    runs vokaturi to the wav file
    :param wav_path: wav file
    :return: vokaturi features dataframe
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    sys.path.append(parallel_run_settings['Vokaturi_API_Path'])
    import Vokaturi

    print("Reading sound file...")
    file_name = wav_path
    (sample_rate, samples) = scipy.io.wavfile.read(file_name)
    print("   sample rate %.3f Hz" % sample_rate)

    print("Allocating Vokaturi sample array...")
    buffer_length = len(samples)
    print("   %d samples, %d channels" % (buffer_length, samples.ndim))
    c_buffer = Vokaturi.SampleArrayC(buffer_length)
    if samples.ndim == 1:
        c_buffer[:] = samples[:] / 32768.0  # mono
    else:
        c_buffer[:] = 0.5 * (samples[:, 0] + 0.0 + samples[:, 1]) / 32768.0  # stereo

    print("Creating VokaturiVoice...")
    voice = Vokaturi.Voice(sample_rate, buffer_length)

    print("Filling VokaturiVoice with samples...")
    voice.fill(buffer_length, c_buffer)

    print("Extracting emotions from VokaturiVoice...")
    quality = Vokaturi.Quality()
    emotion_probabilities = Vokaturi.EmotionProbabilities()
    voice.extract(quality, emotion_probabilities)

    emotions = {}

    if quality.valid:
        print("Neutral: %.3f" % emotion_probabilities.neutrality)
        print("Happy: %.3f" % emotion_probabilities.happiness)
        print("Sad: %.3f" % emotion_probabilities.sadness)
        print("Angry: %.3f" % emotion_probabilities.anger)
        print("Fear: %.3f" % emotion_probabilities.fear)

        emotions['neutrality'] = emotion_probabilities.neutrality
        emotions['happiness'] = emotion_probabilities.happiness
        emotions['sadness'] = emotion_probabilities.sadness
        emotions['anger'] = emotion_probabilities.anger
        emotions['fear'] = emotion_probabilities.fear

    voice.destroy()
    return emotions


def run_vokaturi(video_name_1, video_name_2, parallel_run_settings):
    '''
    Extract vokaturi features and stores in a table
    :return: rows with no emotions
    '''
    # parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    # Mark - add a condition that stops the function from running again if file exists
    if os.path.exists(str(pathlib.Path(os.path.join(parallel_run_settings['csv_path'], video_name_1 + '_' + video_name_2, 'Stage_1', 'talkturn_vokaturi.csv')))):
        return print('Stage 1 Vokaturi File Exists')

    sys.path.append(parallel_run_settings['Vokaturi_API_Path'])
    import Vokaturi

    print("Loading library...")
    Vokaturi.load(parallel_run_settings['Vokaturi_Library_Path'])
    print("Analyzed by: %s" % Vokaturi.versionAndLicense())

    cwf.cut_files(video_name_1, video_name_2, parallel_run_settings)
    no_emotions = []
    emotions = pd.DataFrame()

    # Get the list of talkturn wav
    for root, dirs, files in tqdm(os.walk(parallel_run_settings['talkturn_wav_path'])):
        for file in files:
            if file.endswith('.wav') and not file.startswith('._'):
                print(file)
                talkturn_id_complete = file[0:-4]
                full_path = os.path.abspath(os.path.join(root, file))
                result = analyze_wav(full_path, parallel_run_settings=parallel_run_settings)
                if result:
                    df_emotions = pd.DataFrame(result, index=[0])
                    df_emotions['video_id'] = talkturn_id_complete[0:
                                                                   (len(talkturn_id_complete.split("_")[2]) + 1)*-1]
                    df_emotions['speaker'] = df_emotions.apply(lambda x:
                                                               cfg.parameters_cfg['speaker_1']
                                                               if x['video_id'] == video_name_1
                                                               else cfg.parameters_cfg['speaker_2'],
                                                               axis=1)
                    df_emotions['talkturn no'] = talkturn_id_complete.split("_")[3] #change from 2 to 3
                    df_emotions['audio_id'] = talkturn_id_complete.split("_")[0]
                    df_emotions = df_emotions[
                        ['video_id', 'audio_id', 'speaker', 'talkturn no', 'neutrality', 'happiness',
                         'sadness', 'anger', 'fear']]
                    emotions = emotions.append(df_emotions)
                else:
                    no_emotions.append(full_path)

    print(no_emotions)
    insert_df(emotions_df=emotions, dest_dir=os.path.join(parallel_run_settings['csv_path'],
                                                          video_name_1 + "_" + video_name_2,
                                                          "Stage_1"))

    # Delete files
    files = glob.glob(parallel_run_settings['talkturn_wav_path'] + '\*.wav')

    for f in files:
        try:
            os.unlink(f)
            print("Deleted: " + f)
        except OSError as e:
            print("Error: %s : %s" % (f, e.strerror))


if __name__ == '__main__':
    run_vokaturi(video_name_1="Ses01F_F", video_name_2="Ses01F_M")
