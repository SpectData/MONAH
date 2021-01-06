'''
This script returns a list of wav files to be analyzed
'''


def download_audio(video_name_1, video_name_2):
    '''
    Lists the .wav files from the source folder
    :param dest_dir: folder where the files are saved
    :return: list of names of .wav files
    '''

    # List all audio downloaded
    audio_list = [video_name_1+'.wav', video_name_2+'.wav']

    return audio_list


if __name__ == '__main__':
    audio_list = download_audio(video_name_1, video_name_2)
    print(audio_list)
