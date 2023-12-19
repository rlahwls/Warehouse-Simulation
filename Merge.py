from sqlalchemy import text
import pandas as pd
import Inboundplan_data_create as inb
username = 'root'
password = '1234567'
hostname = 'localhost'
port = '3306'
database = 'warehouse'
db_engine, db_connection = inb.connect_mysql(username,password,hostname,port,database)

def Merge(db_connection):
    # InvID 열이 존재하는지 확인하는 쿼리
    check_InvID_query = text("SHOW COLUMNS FROM inventory LIKE 'InvID'")
    # InvID 열이 존재하는지 여부 확인
    InvID_exists = db_connection.execute(check_InvID_query).fetchone() is not None
    if not InvID_exists:
        # 'InvID' 열이 없을 때 추가하는 쿼리
        add_InvID_query = text('ALTER TABLE inventory ADD COLUMN InvID INT AUTO_INCREMENT PRIMARY KEY FIRST, AUTO_INCREMENT=0')
    else:
        # 'InvID' 열이 이미 존재할 때 수정(변경)하는 쿼리
        add_InvID_query = text('ALTER TABLE inventory MODIFY COLUMN InvID INT AUTO_INCREMENT PRIMARY KEY FIRST, AUTO_INCREMENT=0')
        # 'InvID' 열 추가 실행
        try:
            db_connection.execute(add_InvID_query)
            print('InvID 열이 추가되었습니다.')
        except Exception as e:
            print(f'오류 발생: {e}')
    dfQuery = 'Select INV.Location, INV.ModelCode, IM.Volume,INV.Qty * IM.Volume as InvVol,\
    LOC.MaxVolume  from inventory INV join Locationmaster LOC on INV.Location = LOC.Location \
    join itemmaster IM on INV.ModelCode = IM.ModelCode ;'

    InvQuery = 'Select * from Inventory '

    df = pd.read_sql(text(dfQuery), con=db_connection) 
    Inv = pd.read_sql(text(InvQuery), con=db_connection)
    pr_lst = []
    for i in range(len(Inv)):
        n = 1
        # 잦은 인덱스 수정으로 인한 인덱스 호출 오류 방지
        while i+n <= len(df) :     
            try :    
                if df['ModelCode'][i] == df['ModelCode'][i+n] :
                    a = df['InvVol'][i] # 오류방지용
                    b = df['InvVol'][i+n]
                    InvVolSum = int(a + b) #머지한 볼륨
                    BigMaxVol = max(df['MaxVolume'][i],df['MaxVolume'][i+n]) 
                    if  BigMaxVol == df['MaxVolume'][i] : # 두 로케이션중 더 큰 곳(행수정)과 작은 곳(행삭제)
                        BigI = i
                        DropI = i+n
                    else:
                        BigI = i+n
                        DropI = i
                    if  InvVolSum <= BigMaxVol :
                        pr = '{} and {} were merged at {}. Model : {} Qty : {}'.format(df['Location'][i],\
                        df['Location'][i+n],df['Location'][BigI],df['ModelCode'][BigI], int(InvVolSum/df['Volume'][BigI]))# 머지할 위치 언급                                   
                        pr_lst.append(pr)
                        Inv.loc[BigI,'Qty'] = InvVolSum/(df['Volume'][BigI])
                        Inv.drop(DropI, inplace = True)
                        df.loc[BigI,'InvVol'] = InvVolSum
                        df.drop(DropI, inplace = True)
                        n += 1
                    else :
                        n += 1
                        
                else :
                    n += 1                                
            except KeyError:
                a = None
                b = None
                n += 1
    Inv.to_sql('inventory', con=db_connection, if_exists='replace', index=False)
    
    if pr_lst == [] : 
        pr_lst = 'Merge is impossible'
    else :
        pr_lst = '\n\n'.join (pr_lst)
    return pr_lst

if __name__ == '__main__':
    Merge()





