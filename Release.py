from sqlalchemy import text 
import pandas as pd  
import Inboundplan_data_create as inb
username = 'root'
password = '1234567'
hostname = 'localhost'
port = '3306'
database = 'warehouse'
db_engine, db_connection = inb.connect_mysql(username,password,hostname,port,database)  

def Release(date) : 
    OBdataQuery = f'SELECT * from outboundplan where OutboundDate = "{date}"'
    OBdata = pd.read_sql(text(OBdataQuery), con=db_connection)
    PRT = []
    FAIL = []
    for i in range(len(OBdata['ModelCode'])) : # 출고 리스트 순회
        OModelCode = OBdata['ModelCode'][i]
        OrderID = OBdata['OrderID'][i]
        CheckQuery = f" SELECT ModelCode from inventory where ModelCode = '{OModelCode}' "
        Inv_Query = f"SELECT * FROM inventory "
        Check_Data = pd.read_sql(text(CheckQuery), con=db_connection)
        Inv_Data = pd.read_sql(text(Inv_Query), con=db_connection)
        if Check_Data.empty : # 인벤토리에 이 아이템 없으면
            prt = f" OrderID : {OrderID}, Release failed because {OModelCode} was not in warehouse \\n"  # 출고 실패 
            FAIL.append(prt)
        else : # 인벤토리에 이 아이템 있으면
            Drop_Inv_List = []
            for j in range(len(Inv_Data['Qty'])): # 인벤토리 순회
                Inv_Model = Inv_Data.loc[j,'ModelCode']
                if Inv_Model == OModelCode : # 동일 아이템 찾기
                    Inv_Loc = Inv_Data.loc[j,'Location']
                    OQTY = int(OBdata.loc[i,'QTY'])
                    IQty = int(Inv_Data.loc[j,'Qty'])
                    if OQTY <= IQty: # 재고 충분
                        IQty = IQty - OQTY #재고 수량 업데이트
                        PQTY = OQTY
                        OQTY = 0 # 출고 수량 업데이트           
                        prt = f' OrderID : {OrderID}. {OModelCode} release is sucessfully completed at {Inv_Loc} \\n'
                        PRT.append(prt)             
                    else :    
                        PQTY = IQty
                        OQTY = OQTY - IQty #출고 수량 업데이트 
                        IQty = 0 #재고 수량 업데이트 
                        prt = f" OrderID : {OrderID}. Pick  {OModelCode} {PQTY}ea at {Inv_Loc} \\n"
                    Inv_Data.loc[j,'Qty'] = IQty # Inv 수량 데프 업데이트               
                    if IQty == 0 : # 재고 0 되면
                        Drop_Inv_List.append(j) # Inv 데프 삭제 행 추가
                    if OQTY > 0 :
                        prt = f' OrderID : {OrderID}, Release of {OModelCode} failed as many as {OQTY} \\n'
                        FAIL.append(prt)
                    PRT.append(prt)
                    if OQTY == 0:
                        break
            Inv_Data = Inv_Data.drop(index = Drop_Inv_List).reset_index(drop= True)
            Inv_Data.to_sql('inventory', con=db_engine, if_exists='replace', index=False)
    FF = PRT + FAIL   
    FF = ''.join(FF)        
    return FF    

if __name__ == '__main__' :   
    date = input()
    print(Release(date))

