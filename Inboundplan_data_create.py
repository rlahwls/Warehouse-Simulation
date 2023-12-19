import random
from sqlalchemy import create_engine, text
import pandas as pd

username = 'root'
password = '1234567'
hostname = 'localhost'
port = '3306'
database = 'warehouse'

def connect_mysql(username,password,hostname,port,database):
    db_connection_str = f'mysql+pymysql://{username}:{password}@{hostname}:{port}/{database}'
    db_engine = create_engine(db_connection_str,connect_args={"autocommit": True})
    db_connection = db_engine.connect()
    return db_engine, db_connection
#ID열 생성
def Create_of_Modify_Id_Col(table_name,db_connection):
    # ID 열이 존재하는지 확인
    checkid_query = text(f'SHOW COLUMNS FROM {table_name} LIKE "ID"')
    if not db_connection.execute(checkid_query).fetchone() is not None : 
        # ID' 열이 없을 때 추가
        add_id_query = f'ALTER TABLE {table_name} ADD COLUMN ID INT AUTO_INCREMENT PRIMARY KEY FIRST'  
        try:
            db_connection.execute(text(add_id_query))
            db_connection.commit()
            print('ID 열이 추가되었습니다.')
        except Exception as e:
                print(f'ID열 생성 중 오류 발생: {e}')     
#칼럼 별 row 수 랜덤 설정
def inb_row_num() : 
    total_row_num = random.randint(6,10) #1회당 생성되는 총 row 수(랜덤)
    ref_row = total_row_num
    ref_row_lst = []
    while ref_row > 0:  #1회당 생성되는 중복가능한 row 수(랜덤)
        n = random.randint(1,min(4,ref_row)) 
        ref_row -= n   
        ref_row_lst.append(n)

    ctn_row_lst = []
    for i in ref_row_lst:
        while i > 0:
            n = random.randint(1,i)
            i -= n          
            ctn_row_lst.append(n)
    return total_row_num, ref_row_lst, ctn_row_lst
#SQL에서 테이블의 가장 마지막 행 불러오기
def import_table_last_col(table,col='',db_connection=None):
    select_query = text(f'SELECT * from {table} ORDER BY {col} DESC limit 1;')
    return pd.read_sql(select_query, con=db_connection)

def import_table(table,db_connection=None):
    select_query = text(f'SELECT * from {table};')
    return pd.read_sql(select_query, con=db_connection)


def get_num_data(table_dataframe='', st1='', list1='') :
    lst=[]
    if table_dataframe is not None and not table_dataframe.empty:
        no = int(table_dataframe[f'{st1}No'].iloc[-1][-6:]) +1
    else :
        no = 1
    for i in list1:
        for _ in range(i):
            li = f'{st1}'+str(no).zfill(6)
            lst.append(li)
        no +=  1
    num = pd.DataFrame({f'{st1}No': lst})
    return num

def get_inb_date_data(inbdate,total_row_num):
    inbdate = inbdate
    lst = []
    for i in range(total_row_num):
        lst.append(inbdate)
    Inb_Date_Data = pd.DataFrame({'InboundDate': lst})
    return Inb_Date_Data
def get_model_data(total_row_num) :
    ItemMaster = import_table('itemmaster',db_connection)
    lst = []
    for i in range(total_row_num):
        rand_index = random.randint(0, len(ItemMaster) - 1)
        model_code = ItemMaster['ModelCode'].iloc[rand_index]
        lst.append(model_code)
    Model_Data = pd.DataFrame({'ModelCode': lst})
    return Model_Data
def get_qty_data(total_row_num):
    lst=[]
    for i in range(total_row_num):
        Qty = random.randint(10,30)
        lst.append(Qty)
        Qty_Data = pd.DataFrame({'QTY': lst})
    return Qty_Data
db_engine, db_connection = connect_mysql(username,password,hostname,port,database)
if __name__ == "__main__" :
    while True:
        InbDate = input('날짜를 입력하세요 ex) 1999-10-01 : ')
        if len(InbDate) != 10 or InbDate[4] != '-' or InbDate[7] != '-':
            print('날짜 형식이 올바르지 않습니다. yyyy-mm-dd 형식으로 입력하세요.')
        else :     
            break

    
    Create_of_Modify_Id_Col('inboundplan',db_connection)
    ItemMaster = import_table('itemmaster',db_connection)
    inbound_plan = import_table_last_col('inboundplan','ID',db_connection)

    total_row_num, ref_row_lst, ctn_row_lst = inb_row_num()
    RefNo = get_num_data(inbound_plan,'Ref',ref_row_lst)
    CtnNo = get_num_data(inbound_plan,'Ctn',ctn_row_lst)
    Inb_Date_Data = get_inb_date_data(InbDate,total_row_num)
    Model_Data = get_model_data(total_row_num)
    Qty_Data = get_qty_data(total_row_num)
    # 기존 데이터프레임에 새로운 데이터프레임을 추가, 업데이트

    inbound_plan = RefNo
    inbound_plan['CtnNo'] = CtnNo
    inbound_plan['InboundDate'] = Inb_Date_Data
    inbound_plan['ModelCode'] = Model_Data
    inbound_plan['QTY'] = Qty_Data

    print(inbound_plan)
    # 'inbound_plan' 데이터프레임을 'inboundplan' 테이블에 업데이트
    inbound_plan.to_sql('inboundplan', con=db_engine, if_exists='append', index=False)
    db_connection.commit()
    db_connection.close()