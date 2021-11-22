from functools import reduce

import pandas as pd
import pymysql
import sqlalchemy
import sshtunnel as sshtunnel
from mysql.connector import Error

from loader.ldaps_variable import LDAPS_SOLAR_DEFAULT


def updateDB(df):
    try:
        lst = ['plant_id', 'cdate', 'ctime']
        lst.extend(list(LDAPS_SOLAR_DEFAULT.values()))
        lst.extend(['base_time'])
        df = df[lst]
        df['reg_time'] = pd.Timestamp.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')
        with sshtunnel.SSHTunnelForwarder(
                ('52.78.50.187', 22),
                ssh_username='ec2-user',
                ssh_private_key="/home/LDAPS/custom_vpc.pem",
                remote_bind_address=('redi.cv32wpb0ygub.ap-northeast-2.rds.amazonaws.com', 3306)) as server:
            conn = pymysql.connect(
                host='127.0.0.1',
                port=server.local_bind_port,
                user='redi',
                password='redi2019!!',
                db='enerdot'
            )
            cursor = conn.cursor()
            columns = reduce(lambda x, y: x + ',' + y, df.columns)
            cursor.executemany(
                f'REPLACE INTO gu_research_plant_fcst({columns}) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                df.values.tolist())
            conn.commit()
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
    return df


def getTargPlant(company_lst):
    try:
        company_lst = reduce(lambda x, y: "'" + x + "','" + y + "'", company_lst)
        with sshtunnel.SSHTunnelForwarder(
                ('52.78.50.187', 22),
                ssh_username='ec2-user',
                ssh_private_key="/home/LDAPS/custom_vpc.pem",
                remote_bind_address=('redi.cv32wpb0ygub.ap-northeast-2.rds.amazonaws.com', 3306)) as server:
            conn = sqlalchemy.create_engine(
                'mysql://{0}:{1}@{2}:{3}'.format('redi', 'redi2019!!', '127.0.0.1', server.local_bind_port))
            df = pd.read_sql_query(f"SELECT * FROM enerdot.gu_research_plant where company in ({company_lst});", conn,
                                   index_col='id')

    except Error as e:
        print(e)
    finally:
        conn.dispose()
    return df
