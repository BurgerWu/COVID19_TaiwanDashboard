import psycopg2
import pandas as pd

def drop_create(cur, conn):
    drop_table_query1 = "DROP TABLE IF EXISTS covid19_cases_daily"
    drop_table_query2 = "DROP TABLE IF EXISTS covid19_suspect_daily"
    
    create_table_query1 = """
    CREATE TABLE IF NOT EXISTS covid19_cases_daily 
    (id SERIAL PRIMARY KEY,
    個案研判日 date NOT NULL,
    縣市 text,
    性別 text,
    境外移入 text,
    年齡層 text,
    確定病例數 int NOT NULL)"""

    create_table_query2 = """
    CREATE TABLE IF NOT EXISTS covid19_suspect_daily 
    (id SERIAL,
    通報日 date PRIMARY KEY,
    擴大監測送驗 int,
    居家檢疫送驗 int,
    法定傳染病通報 int,
    Total int NOT NULL)"""
    
    cur.execute(drop_table_query1)
    cur.execute(drop_table_query2)
    cur.execute(create_table_query1)
    cur.execute(create_table_query2)
    conn.commit()
    
    print('Create covid 19 tables successfully')    
    

def insert_table(cur, conn, df_cases, df_suspect):  
    
    query1 = """
    INSERT into covid19_cases_daily (個案研判日, 縣市, 性別, 境外移入, 年齡層, 確定病例數) 
    VALUES(%s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    query2 = """
    INSERT into covid19_suspect_daily (通報日, 擴大監測送驗, 居家檢疫送驗, 法定傳染病通報, Total) 
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (通報日) DO UPDATE SET 
    擴大監測送驗 = EXCLUDED.擴大監測送驗, 
    居家檢疫送驗 = EXCLUDED.居家檢疫送驗,
    法定傳染病通報 = EXCLUDED.法定傳染病通報,
    Total = EXCLUDED.Total;
    """
    
    if df_cases.shape[0] > 0:
        for index, row in df_cases.iterrows():
            insert_cases = [row.個案研判日, row.縣市, row.性別, row.是否為境外移入, row.年齡層, row.確定病例數]
            cur.execute(query1, insert_cases)
            conn.commit()
        print('Successfully insert cases data to database')

    if df_suspect.shape[0] > 0:
        for index, row in df_suspect.iterrows():
            insert_suspect = [row.通報日, row.擴大監測送驗, row.居家檢疫送驗, row.法定傳染病通報, row.Total]
            cur.execute(query2, insert_suspect)
            conn.commit()    
        print('Successfully insert suspect data to database')

def main():
    print("Connecting to Postgresql server")
    conn = psycopg2.connect("host=127.0.0.1 dbname=COVID19_Taiwan user=postgres  password=aritek168")
    cur = conn.cursor()
    
    print("Obtaining target dataframe")
    df_case = pd.read_json('https://od.cdc.gov.tw/eic/Day_Confirmation_Age_County_Gender_19CoV.json')
    df_suspect = pd.read_csv('https://od.cdc.gov.tw/eic/covid19/covid19_tw_specimen.csv')

    print("Drop and create dataframe")
    drop_create(cur, conn)
    
    print("Insert dataframe")
    insert_table(cur, conn, df_case, df_suspect)    
    
    print("Inserting data complete")
    conn.close()
    
main()


