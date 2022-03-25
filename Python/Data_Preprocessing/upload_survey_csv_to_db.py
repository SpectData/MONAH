import Python.Data_Preprocessing.config.sql_connection as SC
import pandas as pd

def upload_csv_to_db(full_path_to_file, table_name):
        df = pd.read_csv(full_path_to_file)
        df = df.fillna(value='')
        df = df.iloc[2:]
        print(df)
        cnxn = SC.get_connection()
        cursor = cnxn.cursor()
        sql = 'TRUNCATE TABLE qualtrics.' + table_name
        cursor.execute(sql)
        cnxn.commit()
        cursor.close()
        cnxn.close()

        # Insert row values to the db
        data = df.values.tolist()
        sql = 'INSERT INTO qualtrics.' + table_name + ' VALUES({0})'
        sql = sql.format(','.join('?' * len(df.columns)))
        cnxn = SC.get_connection()
        cursor = cnxn.cursor()
        number_of_rows = cursor.executemany(sql, data)
        cnxn.commit()
        cursor.close()
        cnxn.close()

        print('data successfully inserted.')


if __name__=='__main__':
    upload_csv_to_db(full_path_to_file='E:/MONAH/responses.csv', table_name='responses') # responses
    # upload_csv_to_db(full_path_to_file='E:/MONAH/signups.csv', table_name='signups') # sign_ups