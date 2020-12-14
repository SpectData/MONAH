'''
This script extracts the audio recording from the video file and stores it in a wav file
'''
import logging
import os

import Python.Data_Preprocessing.config.dir_config as prs


def run_extracting_audio():
    '''
    Extract audio only from the video file
    :return: none
    '''
    logging.getLogger().setLevel(logging.INFO)

    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")

    video_list = []
    for file in os.listdir(parallel_run_settings['avi_path']):
        if file.endswith(".avi"):
            video_list.append(file)

    for video_i in video_list:
        video_path = os.path.join(parallel_run_settings['avi_path'], video_i)
        video_path = os.path.splitext(video_path)[0]
        os.system("ffmpeg -i {0}.avi -f wav -ab 192000 -vn {0}.wav".format(video_path))

        log_text = "Completed extracting audio for "
        logging.info(log_text + video_i)

if __name__ == "__main__":
    run_extracting_audio()
