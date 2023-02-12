import yfinance as yf
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def getConnection():
    conn_string = 'postgresql://postgres:password@localhost:5432/postgres'
    db = create_engine(conn_string)
    conn = db.connect()
    conn1 = psycopg2.connect(
        database="postgres",
        user='postgres',
        password='password',
        host='localhost',
        port='5432')
    conn1.autocommit = True
    cursor = conn1.cursor()
    return conn,conn1, cursor

countries = pd.read_csv('companies.csv')

conn, conn1, cursor = getConnection()
cursor.execute('''drop table if exists companies_table;''')

sql = """CREATE TABLE IF NOT EXISTS companies_table (
    Id INT GENERATED ALWAYS AS IDENTITY,
    symbol text,
    updated_timestamp timestamp,
    name text,
    country text,
    industry text);"""

cursor.execute(sql)


countries['updated_timestamp'] = pd.Timestamp('now')

countries.rename(columns={'updated_timestamp': 'updated_timestamp',
                           'Industry': 'industry',
                           'Name': 'name',
                           'Country': 'country',
                           'Symbol': 'symbol'}, inplace=True)

countries.to_sql('temp', conn, if_exists='replace')

insert_content = '''insert into companies_table(updated_timestamp, industry, name, country, symbol)
select updated_timestamp, industry, name, country, symbol from temp'''

drop_temp = '''drop table temp;'''

cursor.execute(insert_content)
cursor.execute(drop_temp)

conn1.commit()
conn1.close()


