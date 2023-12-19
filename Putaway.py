import pandas as pd
from sqlalchemy import text
# 데이터베이스 연결 설정
import Inboundplan_data_create as inb
username = 'root'
password = '1234567'
hostname = 'localhost'
port = '3306'
database = 'warehouse'
db_engine, db_connection = inb.connect_mysql(username,password,hostname,port,database)  

def update_if_find_new_loc(db_connection,item, item_volume, item_qty, zonecode):
    new_location_list_query = f"SELECT Location FROM locationmaster WHERE ZoneCode = '{zonecode}' AND Location NOT IN (SELECT Location FROM inventory)"
    new_location = pd.read_sql(text(new_location_list_query), con=db_connection).reset_index(drop=True)['Location'][0]
    MaxVolumeQuery = f'SELECT MaxVolume from locationmaster where Location = "{new_location}"'    
    location_max_volume = int(pd.read_sql(text(MaxVolumeQuery), con=db_connection)['MaxVolume'].iloc[0])
    location = new_location
    # inbvolume < location max volume
    if item_volume * item_qty <= location_max_volume : # 로케이션에 다 들어갈 때
        qty =item_qty
        item_qty = 0 
    elif item_volume * item_qty > location_max_volume : #로케이션에 다 안들어 갈 때
        qty =  location_max_volume// item_volume
        item_qty -= qty
    insert_query = f"INSERT INTO inventory (Location, ModelCode, Qty) VALUES ('{location}','{item}', {qty})"
    db_connection.execute(text(insert_query))
    return item_qty

def process_inbound_items(db_connection, date): #가장 중요한거
    inb_item_query = f"SELECT ModelCode, QTY FROM inboundplan WHERE InboundDate = '{date}'"
    inb_item_result = pd.read_sql(text(inb_item_query), con=db_connection)
    for i in range(len(inb_item_result['ModelCode'])):
        item = inb_item_result['ModelCode'][i]
        item_volume = int(pd.read_sql(f"SELECT Volume FROM itemmaster WHERE ModelCode = '{item}'", con=db_connection)['Volume'][0])
        item_qty = int(inb_item_result['QTY'][i]) 
        while item_qty > 0:
            result = process_inbound_single_item(db_connection,item, item_volume, item_qty)
            item_qty = result

def process_inbound_single_item(db_connection, item, item_volume, item_qty):
    if item_volume <= 6 : #볼륨 6 이하면 smallzone (끝)
        ZoneCode = 'Small'
    else : 
        ZoneCode = 'Bulk'
    item_inventory_query = f"""SELECT I.Location, I.Qty AS InvQty, L.MaxVolume, P.Qty as PlanQty, I.ModelCode from inventory I
Join locationmaster L ON I.Location = L.Location
Join inboundplan P ON P.Modelcode = I.ModelCode
WHERE P.ModelCode = '{item}'"""
    location_result = pd.read_sql(text(item_inventory_query), con=db_connection)
    if not location_result.empty : # 이미 인벤토리에 존재할 경우 
        for j in range(len(location_result['Location'])):
            left_loc_volume = int(location_result['MaxVolume'][j]) - int(location_result['InvQty'][j])*item_volume
            lef_inv_vol = item_qty * item_volume
            location =  location_result['Location'][j]
            if left_loc_volume < lef_inv_vol : # 남은 공간 볼륨 < 남은 아이템 볼륨 (전체가 들어갈 공간이 없을때)
                if left_loc_volume >= item_volume :  #한개라도 들어갈 때
                    qty = left_loc_volume//item_volume 
                    item_qty -= qty
                    update_query = f"UPDATE inventory SET Qty = {location_result['MaxVolume'][j]//item_volume} WHERE Location = '{location}'"
                    db_connection.execute(text(update_query))  
                else : # 한개도 안들어갈 때
                        item_qty = update_if_find_new_loc(db_connection,item, item_volume, item_qty, ZoneCode)
            elif left_loc_volume >= lef_inv_vol : # (전체 아이템이 들어갈 여유공간 있을때)
                qty = int(location_result['InvQty'][j])+item_qty
                item_qty = 0
                update_query = f"UPDATE inventory SET Qty = {qty} WHERE Location = '{location}'"
                db_connection.execute(text(update_query))
    else : # 첫 인바운드일 경우 (끝)
        item_qty = update_if_find_new_loc(db_connection,item, item_volume, item_qty,ZoneCode)
    return item_qty
        

if __name__ == '__main__':
    date = input('날짜를 입력하세요 ex) 1234-12-12 : ')
    process_inbound_items(db_connection, date)