'''
This script extracts the audio recording from the video file and stores it in a wav file
'''
import logging
import os
import pathlib
import shutil


def run_extracting_audio(video_name_1, video_name_2, parallel_run_settings):
    '''
    Extract audio only from the video file
    :return: none
    '''
    file_count = 0
    for i in [video_name_1, video_name_2]:
        if os.path.exists(str(pathlib.Path(os.path.join(parallel_run_settings['wav_path'], i + '.wav')))):
            print('Stage 1 Audio File: ' + i + ' Exists')
            file_count += 1
    if file_count == 2:
        return print('Stage 1 Both Audio Files Exist')

    logging.getLogger().setLevel(logging.INFO)

    video_list = []
    for file in os.listdir(parallel_run_settings['avi_path']):
        if file.endswith(".avi"):
            video_list.append(file)

    video_i = video_list[0]
    for video_i in video_list:
        video_path = os.path.join(parallel_run_settings['avi_path'], video_i)
        video_path = os.path.splitext(video_path)[0]
        os.system("ffmpeg -i {0}.avi -f wav -ab 192000 -ac 1 -vn {0}.wav".format(video_path))

        shutil.move(video_path + '.wav',
                    parallel_run_settings['wav_path'] + '/' + os.path.splitext(video_i)[0] + '.wav')

        log_text = "Completed extracting audio for "
        logging.info(log_text + video_i)

if __name__ == "__main__":
    run_extracting_audio()
