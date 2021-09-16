import psycopg2
import pandas as pd
import datetime

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
    
    
def update_table(cur, conn):
    cur.execute("""SELECT max(個案研判日) FROM covid19_cases_daily""")
    results1 = cur.fetchone()[0]
    df1 = pd.read_json('https://od.cdc.gov.tw/eic/Day_Confirmation_Age_County_Gender_19CoV.json')
    df1['個案研判日'] = pd.to_datetime(df1['個案研判日'])
    to_update_case = df1[df1['個案研判日'] > datetime.datetime(results1.year, results1.month, results1.day) - datetime.timedelta(3)]
    
    print('Covid19 cases daily dataset updated')
    
    cur.execute("""SELECT max(通報日) FROM covid19_suspect_daily""")
    results2 = cur.fetchone()[0]
    df2 = pd.read_csv('https://od.cdc.gov.tw/eic/covid19/covid19_tw_specimen.csv')
    df2['通報日'] = pd.to_datetime(df2['通報日'])
    to_update_suspect = df2[df2['通報日'] > datetime.datetime(results2.year, results2.month, results2.day) - datetime.timedelta(3)]
    
    insert_table(cur, conn, to_update_case, to_update_suspect)
    print('Covid19 suspect daily dataset updated')
    
    
def main():
    print("Connecting to Postgresql server")
    conn = psycopg2.connect("host=127.0.0.1 dbname=COVID19_Taiwan user=postgres  password=aritek168")
    cur = conn.cursor()
    update_table(cur, conn)  
    conn.close()
        
main()