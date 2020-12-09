'''
This script uploads wav files to be analyzed to google cloud
'''

import os
from google.cloud import storage
import data.secrets.parallel_run_settings_secret as prs

def upload_blob(bucket_name, destination_blob_name):
    '''
    Uploads a file to the bucket.
    :param bucket_name: bucket name in google cloud
    :param source_file_name: local file name of wav file
    :param destination_blob_name: file name in google cloud
    :return: none
    '''
    parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./data/secrets/69b431b78607.json"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(os.path.join(parallel_run_settings['wav_path'],
                                           destination_blob_name))

if __name__ == '__main__':
    upload_blob(bucket_name="marriane-bucket",
                destination_blob_name="operator_poppy.wav")
