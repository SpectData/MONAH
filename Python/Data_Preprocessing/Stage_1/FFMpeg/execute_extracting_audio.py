'''
This script extracts the audio recording from the video file and stores it in a wav file
'''
import logging
import os
import shutil
import Python.Data_Preprocessing.config.dir_config as prs


def run_extracting_audio(parallel_run_settings):
    '''
    Extract audio only from the video file
    :return: none
    '''
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
    parallel_run_settings = prs.get_parallel_run_settings("marriane_linux")
    run_extracting_audio(parallel_run_settings=parallel_run_settings)
