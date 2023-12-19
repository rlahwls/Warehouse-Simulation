import random
import string
from sqlalchemy import text
import inboundplan_data_create as inb
username = 'root'
password = '1234567'
hostname = 'localhost'
port = '3306'
database = 'warehouse'
db_engine, db_connection = inb.connect_mysql(username,password,hostname,port,database)


# 랜덤한 OrderID 생성 함수
def generate_order_id(order_count):
    return f'Order{str(order_count).zfill(6)}'

# 랜덤한 문자열 생성 함수
def random_string(length):
    letters = string.ascii_uppercase
    return ''.join(random.choice(letters) for _ in range(length))

# 랜덤한 Outbound 데이터 생성 함수
def generate_outbound_data(db_connection, date, order_count):
    total_generated_rows = 0  # 총 생성된 행 수를 저장하는 변수
    # 랜덤한 행 수 생성 (6~10)
    num_rows = random.randint(6, 10)
    while total_generated_rows < num_rows:
        # 랜덤한 중복 OrderID 개수 생성 (1~6)
        num_order_ids = random.randint(1, 6)
        if num_order_ids == 1:
            break
        for _ in range(num_order_ids):
            order_id = generate_order_id(order_count)
            # 중복 OrderID 내의 중복 모델코드 개수 생성 (1~4)
            num_item_models = random.randint(1, 4)
            for _ in range(num_item_models):
                # 랜덤한 QTY 생성 (1~50)
                qty = random.randint(1, 20)
                # RefNo 생성 (랜덤한 문자열)
                ref_no = random_string(6)              
                # 랜덤한 ModelCode 조회 (인벤토리에 존재하는 ModelCode 중 하나 선택)
                modelcode_query = text("SELECT ModelCode FROM inventory ORDER BY RAND() LIMIT 1")
                modelcode_result = db_connection.execute(modelcode_query)
                modelcode = modelcode_result.fetchone()[0] # 첫번째 행(fetchone), 첫번째 열
                
                # 해당 ModelCode의 총 QTY 조회
                total_qty_query = text(f"SELECT SUM(Qty) FROM inventory WHERE ModelCode = '{modelcode}'")
                total_qty_result = db_connection.execute(total_qty_query)
                total_qty = total_qty_result.fetchone()[0]
                
                if total_qty is None:
                    total_qty = 0
                
                # Outbound 데이터의 QTY가 50을 초과하지 않도록 보정
                if qty > total_qty:
                    qty = total_qty
                
                # Outbound 데이터 삽입
                outbound_insert_query = text(f"""
                    INSERT INTO OutboundPlan (OrderID, OutboundDate, ModelCode, RefNo, Qty)
                    VALUES ('{order_id}', '{date}', '{modelcode}', '{ref_no}', {int(qty)})
                """)
                db_connection.execute(outbound_insert_query)
                total_generated_rows += 1
            
            order_count += 1
    
    return total_generated_rows

# 데이터베이스 연결 설정

if __name__ == '__main__':
    date = input("Outbound 날짜를 입력하세요 (예: 2023-09-01), 종료하려면 'exit' 입력: ")
    inb.Create_of_Modify_Id_Col('outboundplan',db_connection)
    order_data = inb.import_table_last_col('outboundplan', 'OrderID', db_connection)
    if not order_data.empty:
        order_count = int(order_data.iloc[-1, 1][-6:])
    else:
        order_count = 1
    total_generated_rows = generate_outbound_data(db_connection, date, order_count)
    print(f'{total_generated_rows}개의 Outbound 데이터가 생성되었습니다.')
    db_connection.commit()
    # 데이터베이스 연결 종료
    db_connection.close()
