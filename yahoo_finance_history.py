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

conn, conn1, cursor = getConnection()

def getListOfCompanies():
    list_of_companies = list(pd.read_sql(''' select distinct symbol from companies_table;''', conn)['symbol'].values)
    return list_of_companies

cursor.execute('''drop table if exists historical_table;''')

sql = """CREATE TABLE IF NOT EXISTS historical_table (
    Id INT GENERATED ALWAYS AS IDENTITY,
    Symbol text,
    updated_timestamp timestamp,
    Open float,
    High float,
    Low float,
    Close float,
    Volume bigint,
    Stock_Splits bigint,
    date TIMESTAMPTZ);"""

cursor.execute(sql)


count = 0
for i in range(0,len(getListOfCompanies())):
    item = getListOfCompanies()[i]
    company_id = item
    company_id = yf.Ticker(company_id)
    history = company_id.history(period= "max")
    history_df = pd.DataFrame(history)
    history_df.drop(['Dividends'], axis=1)
    history_df['Symbol'] = item
    history_df['updated_timestamp'] = pd.Timestamp('now')
    print(item, count, 'start')


    history_df.rename(columns = {'Stock Splits': 'stock_splits',
                                 'Open': 'open',
                                 'High': 'high',
                                 'Low': 'low',
                                 'Close': 'close',
                                 'Volume': 'volume',
                                 'Symbol': 'symbol'}, inplace = True)
    history_df.index.names = ['date']


    history_df.to_sql('temp', conn, if_exists= 'replace')


    insert_content = '''insert into historical_table(date, open, high, low, close, volume, stock_splits, symbol, updated_timestamp)
select date, open, high, low, close, volume, stock_splits, symbol, updated_timestamp from temp'''

    drop_temp = '''drop table temp;'''

    cursor.execute(insert_content)
    cursor.execute(drop_temp)

    conn1.commit()
    count += 1
    print(item, count, 'finished')


    
conn1.close()


# df = pd.read_sql(sql2, conn)
# print(df)



