'''
1. Avoid incuring Google ASR by downloading the word transcript from my own database
2. Run main, but without Google ASR
3. Upload it back to iemocap.Narrative_Fine_Pitch_Vol
'''

import json
import os

import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient


def readSecret():
    '''
        Reads config from ~/data/secrets/secret.cfg and returns it.
        Config file must look like {"username":"test", "password": "pass", ... }.
        Config file must have following keys: username, password, database, server, driver
    '''

    with open('./data/secrets/secret.cfg') as f:
        return json.loads(f.read())


def get_connection():
    configDic = readSecret()

    driver = configDic['driver']
    server = configDic['server']
    database = configDic['database']
    username = configDic['username']
    password = configDic['password']

    # Creating a connection
    cnxn = pyodbc.connect(f'''DRIVER={driver};
    SERVER={server};
    UID={username};
    PWD={password};
    DATABASE={database};''')
    return cnxn


def get_sqlalchemy_engine():
    import sqlalchemy as sa
    configDic = readSecret()

    driver = configDic['driver']
    driver = driver[1:-1]
    server = configDic['server']
    database = configDic['database']
    username = configDic['username']
    password = configDic['password']
    engine = sa.create_engine(f'mssql://{username}:{password}@{server}/{database}?driver={driver}')

    return engine


def download_all_avi():
    configDic = readSecret()

    blob_service_client = BlobServiceClient.from_connection_string(configDic['storage_blob'])
    container_client = blob_service_client.get_container_client('iemocap-split')

    dir(container_client)

    # List the blobs in the container
    blob_list = container_client.list_blobs()
    local_path = '/mnt/S/researchdata/IEMOCAP/iemocap-split-avi'

    for blob in blob_list:
        if str(blob.name).endswith('.avi'):
            download_file_path = os.path.join(local_path,
                                              blob.name)
            print("\nDownloading blob to \n\t" + download_file_path)
            blob_client = container_client.get_blob_client(blob.name)
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())


def get_keys_to_work_through():
    '''
    Video ID/ primary keys to iterate over
    :return:
    '''
    configDic = readSecret()

    engine = get_sqlalchemy_engine()
    sql = '''
    SELECT DISTINCT video_ID
      FROM [iemocap].[Narrative_Fine2]
      WHERE family ='vpa'
      ORDER BY video_ID
      '''

    left_frame = pd.read_sql(sql, engine)
