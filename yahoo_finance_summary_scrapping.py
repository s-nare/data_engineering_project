import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# import time
# from telnetlib import EC
# from urllib.request import urlopen
# from xml.dom.xmlbuilder import Options
from selenium import webdriver
# from selenium.webdriver import ActionChains
# from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
# from selenium.webdriver.support.wait import WebDriverWait


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

cursor.execute('''drop table if exists summary_table;''')
cursor.execute('''drop table if exists temp;''')

sql = """CREATE TABLE IF NOT EXISTS summary_table (
       Id INT GENERATED ALWAYS AS IDENTITY,
       metric text,
       value text,
       symbol text,
       updated_timestamp timestamp);"""
cursor.execute(sql)
count = 0

for i in range(0, len(getListOfCompanies())):
    item_id = getListOfCompanies()[i]

    url = "https://finance.yahoo.com/quote/" + item_id
    driver_options = webdriver.ChromeOptions()
    driver_options.add_argument('--ignore-certificate-errors')
    driver_options.add_argument('--incognito')
    driver_options.add_argument('--headless')
    driver = webdriver.Chrome()
    driver.get(url)

    soup = BeautifulSoup(driver.page_source, 'lxml')
    tag = soup.find(id="quote-summary")
    tables = tag.findAll("table")
    left_summary_table = tables[0]
    right_summary_table = tables[1]
    left_tds = left_summary_table.findAllNext("td")
    right_tds = right_summary_table.findAllNext("td")

    left_table = []
    right_table = []


    for item in range(0,len(left_tds),2):
        left_table.append((left_tds[item].text.strip(),left_tds[item +1].text.strip()))

    for item in range(0,len(right_tds),2):
        right_table.append((right_tds[item].text.strip(),right_tds[item +1].text.strip()))


    left_df = pd.DataFrame.from_records(left_table)
    right_df = pd.DataFrame.from_records(right_table)

    left_df.rename(columns={0: 'metric',
                            1: 'value'}, inplace=True)
    right_df.rename(columns={0: 'metric',
                            1: 'value'}, inplace=True)
    summary_df = pd.concat([left_df, right_df])
    summary_df['updated_timestamp'] = pd.Timestamp('now')
    summary_df['symbol'] = item_id

    print(item_id, count, 'start')



    summary_df.to_sql('temp', conn, if_exists= 'replace')
    insert_content = '''insert into summary_table(symbol, metric, value, updated_timestamp)
     select symbol, metric, value, updated_timestamp from temp'''

    drop_temp = '''drop table temp;'''
    cursor.execute(insert_content)
    cursor.execute(drop_temp)
    conn1.commit()
    print(item_id, count, 'finished')

    count += 1

conn1.close()
conn.close()







