import os, json
import pyodbc

def readSecret():
    '''
        Reads config from ~/data/secrets/secret.cfg and returns it.
        Config file must look like {"username":"test", "password": "pass", ... }.
        Config file must have following keys: username, password, database, server, driver
    '''

    with open('C:/Users/marriane-f2sh391ja/Documents/GitHub/MONAH/data/secrets/secret.cfg') as f:
        return json.loads(f.read())


def get_connection():
    configDic = readSecret()

    driver = configDic['driver']
    server = configDic['server']
    database = configDic['database']
    username = configDic['username']
    password = configDic['password']

    # Creating a connection
    cnxn = pyodbc.connect(f"""DRIVER={driver};
    SERVER={server};
    UID={username};
    PWD={password};
    DATABASE={database};""")
    return cnxn

def get_sqlalchemy_engine():
    import sqlalchemy as sa
    configDic = readSecret()

    driver = configDic['driver']
    driver=driver[1:-1]
    server = configDic['server']
    database = configDic['database']
    username = configDic['username']
    password = configDic['password']
    engine = sa.create_engine(f'mssql://{username}:{password}@{server}/{database}?driver={driver}')

    return engine

if __name__ == '__main__':
    cnxn = get_connection()
    print(cnxn)