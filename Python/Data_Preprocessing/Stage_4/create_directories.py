'''
This script creates the needed directoriesfor session-level transcripts
'''
import os
import data.secrets.parallel_run_settings_secret as prs

def run_creating_directories(video_name_1, video_name_2):
    '''
    Create directory for session-level transcripts
    :return: none
    '''
    parallel_run_settings = prs.get_parallel_run_settings("marriane_win")
    out_dir = os.path.join(parallel_run_settings['csv_path'],
                           video_name_1 + '_' + video_name_2,
                           'Stage_4')
    for sub_folder in ['Actions', 'Demographics', 'Mimicry', 'Prosody', 'Semantics']:
        sub_dir = os.path.join(out_dir, sub_folder)
        if not os.path.exists(sub_dir):
            os.makedirs(sub_dir)

if __name__ == '__main__':
    run_creating_directories(video_name_1='Ses01F_F', video_name_2='Ses01F_M')
