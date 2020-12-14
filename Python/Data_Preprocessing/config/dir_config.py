'''
This should not be a secrets file. To be renamed as directory_setup
#TODO: Joshua to coordinate the migration of this out of the secrets folder
#TODO: Joshua to ensure that (1) EVERY setting is used
#(2) EVERY setting is populated
----------------------------------

This file sets up the directory paths of all input files, output files and
software dependencies.
'''

import os
import pathlib
import platform


def _auto_populate_derivative_directories(parallel_run_settings):
    # Automatically Populate Derivative directories
    parallel_run_settings['OpenFace_CSV_Path'] = str(pathlib.Path(
        os.path.join(parallel_run_settings['MONAH'], 'staging')
    ))

    parallel_run_settings['avi_path'] = str(pathlib.Path(
        os.path.join(parallel_run_settings['MONAH'], 'downloaded_videos')))
    parallel_run_settings['wav_path'] = str(pathlib.Path(
        os.path.join(parallel_run_settings['MONAH'], 'downloaded_audio')))
    parallel_run_settings['csv_path'] = str(pathlib.Path(
        os.path.join(parallel_run_settings['MONAH'], 'output')))

    parallel_run_settings['usb_csv_path'] = str(pathlib.Path(
        os.path.join(parallel_run_settings['MONAH'], 'open_face_csv')))
    parallel_run_settings['talkturn_wav_path'] = str(
        os.path.join(parallel_run_settings['wav_path'], 'talkturn'))

    parallel_run_settings['Vokaturi_API_Path'] = str(
        pathlib.Path(os.path.join(parallel_run_settings['Vokaturi_Install_Path'], 'api')))

    if 'linux' in platform.platform().lower():
        parallel_run_settings['Vokaturi_Library_Path'] = str(pathlib.Path(
            os.path.join(parallel_run_settings['Vokaturi_Install_Path'],
                         "lib/open/linux/OpenVokaturi-3-4-linux64.so")))

    else:
        parallel_run_settings['Vokaturi_Library_Path'] = str(pathlib.Path(
            os.path.join(parallel_run_settings['Vokaturi_Install_Path'],
                         "lib/open/win/OpenVokaturi-3-4-win64.dll")))

    return parallel_run_settings


def get_parallel_run_settings(computer_name):
    assert (computer_name) in set(['marriane_win',
                                   'marriane_linux',
                                   'joshua_linux']), 'Unknown Computer Name'

    parallel_run_settings = {}
    parallel_run_settings['computer_name'] = computer_name

    # Initialize
    parallel_run_settings['OpenFace_CSV_Path'] = None
    parallel_run_settings['avi_path'] = None
    parallel_run_settings['wav_path'] = None
    parallel_run_settings['csv_path'] = None

    # Setting up OpenFace
    # https://github.com/TadasBaltrusaitis/OpenFace
    parallel_run_settings['feature_extraction_path'] = None
    parallel_run_settings['offset_multiple'] = None
    parallel_run_settings['usb_csv_path'] = None

    # Setting up Vokaturi version 3.4
    # https://developers.vokaturi.com/downloads/sdk
    # This is the path to Vokaturi.py when you download the
    # OpenVokaturi Software Development Kit from the link above
    parallel_run_settings['Vokaturi_Install_Path'] = None
    parallel_run_settings['Vokaturi_API_Path'] = None
    parallel_run_settings['Vokaturi_Library_Path'] = None
    parallel_run_settings['talkturn_wav_path'] = None

    # Populate Core directories
    if computer_name == 'marriane_win':
        parallel_run_settings['MONAH'] = str(pathlib.Path("E:\iemocap"))
        parallel_run_settings['OpenFace_CSV_Path'] = str(pathlib.Path("E:\iemocap\staging"))
        parallel_run_settings['avi_path'] = str(pathlib.Path("E:\iemocap\downloaded_videos"))
        parallel_run_settings['wav_path'] = str(pathlib.Path("E:\iemocap\downloaded_audio"))
        parallel_run_settings['csv_path'] = str(pathlib.Path("E:\iemocap\output"))
        parallel_run_settings['feature_extraction_path'] = str(
            pathlib.Path("E:\OpenFace_2.2.0_win_x64 Josh\OpenFace_2.2.0_win_x64"))
        parallel_run_settings['offset_multiple'] = 0
        parallel_run_settings['usb_csv_path'] = str(pathlib.Path("E:\iemocap\open_face_csv"))
        parallel_run_settings['Vokaturi_Install_Path'] = str(
            pathlib.Path("E:/OpenVokaturi-3-4/OpenVokaturi-3-4"))
        parallel_run_settings['Vokaturi_API_Path'] = str(
            pathlib.Path("E:/OpenVokaturi-3-4/OpenVokaturi-3-4/api"))
        parallel_run_settings['Vokaturi_Library_Path'] = str(pathlib.Path(
            "E:/OpenVokaturi-3-4/OpenVokaturi-3-4/lib/open/win/OpenVokaturi-3-4-win64.dll"))
        parallel_run_settings['talkturn_wav_path'] = str(
            pathlib.Path("E:/iemocap/downloaded_audio/talkturn"))

    if computer_name == 'marriane_linux':
        # TODO: Josh to delete derivative folders after checking that it works on Linux
        parallel_run_settings['MONAH'] = str(pathlib.Path("/datadrive/iemocap"))
        parallel_run_settings['OpenFace_CSV_Path'] = str(pathlib.Path("/datadrive/iemocap/staging"))
        parallel_run_settings['avi_path'] = str(
            pathlib.Path("/datadrive/iemocap/downloaded_videos"))
        parallel_run_settings['wav_path'] = str(pathlib.Path("/datadrive/iemocap/downloaded_audio"))
        parallel_run_settings['feature_extraction_path'] = str(
            pathlib.Path("E:\OpenFace_2.2.0_win_x64 Josh\OpenFace_2.2.0_win_x64"))
        parallel_run_settings['offset_multiple'] = 0
        parallel_run_settings['usb_csv_path'] = str(
            pathlib.Path("/datadrive/iemocap/open_face_csv"))

    if computer_name == 'joshua_linux':
        parallel_run_settings['MONAH'] = str(pathlib.Path("/mnt/S/MONAH"))

        parallel_run_settings['feature_extraction_path'] = str(
            pathlib.Path("/mnt/G/Github/OpenFace"))

        parallel_run_settings['offset_multiple'] = 0

        parallel_run_settings['Vokaturi_Install_Path'] = str(
            pathlib.Path("/mnt/H/Tools/OpenVokaturi-3-4"))

    parallel_run_settings = _auto_populate_derivative_directories(parallel_run_settings)

    return parallel_run_settings
