import pandas as pd
import Inboundplan_data_create as inb
username = 'root'
password = '1234567'
hostname = 'localhost'
port = '3306'
database = 'warehouse'
db_engine, db_connection = inb.connect_mysql(username,password,hostname,port,database)
# 엑셀 파일에서 시트 읽기
excel_file_path = 'C:/Users/82105/Desktop/위대한 프로젝트/Warehouse_Data.xlsx'
df_ItemMaster = pd.read_excel(excel_file_path, sheet_name='itemmaster')
df_LocationMaster = pd.read_excel(excel_file_path, sheet_name='locationmaster')
df_InboundPlan = pd.read_excel(excel_file_path, sheet_name='inboundplan')
df_Inventory = pd.read_excel(excel_file_path, sheet_name='inventory')
df_OutboundPlan = pd.read_excel(excel_file_path, sheet_name='outboundplan')


# DataFrame을 MySQL 테이블로 저장
df_ItemMaster.to_sql('itemmaster', db_engine, if_exists='replace', index=False)
df_LocationMaster.to_sql('locationmaster', db_engine, if_exists='replace', index=False)
df_InboundPlan.to_sql('inboundplan', db_engine, if_exists='replace', index=False)
df_Inventory.to_sql('inventory', db_engine, if_exists='replace', index=False)
df_OutboundPlan.to_sql('outboundplan', db_engine, if_exists='replace', index=False)

# 연결 종료
db_engine.dispose()

