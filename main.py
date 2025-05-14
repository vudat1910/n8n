from fastapi import FastAPI, Request, UploadFile, File
from typing import List
import psycopg2
from psycopg2 import Error
import pandas as pd

# Khởi tạo FastAPI
app = FastAPI()

# Hàm kết nối tới PostgreSQL
def get_db_connection():
    try:
        connection = psycopg2.connect(
            host="localhost",
            port=5432,
            database="your_db",
            user="your_user",
            password="your_password"
        )
        return connection
    except Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# Hàm kiểm tra và điều chỉnh bảng
def adjust_table_if_needed(connection, table_name, data_sample):
    try:
        with connection.cursor() as cursor:
            # Kiểm tra xem bảng có tồn tại không
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                # Tạo bảng mới
                columns = data_sample.columns.tolist()
                column_definitions = ["id SERIAL PRIMARY KEY"]
                for col in columns:
                    column_definitions.append(f"{col} VARCHAR(255)")
                create_table_query = f"""
                    CREATE TABLE {table_name} (
                        {', '.join(column_definitions)},
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table {table_name} created successfully")
            else:
                # Kiểm tra cột hiện tại của bảng
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s;
                """, (table_name,))
                existing_columns = [row[0] for row in cursor.fetchall() if row[0] not in ['id', 'created_at']]

                # Lấy cột từ file XLSX
                new_columns = data_sample.columns.tolist()
                
                # Thêm cột mới nếu cần
                for col in new_columns:
                    if col not in existing_columns:
                        cursor.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN {col} VARCHAR(255);
                        """)
                        print(f"Added column {col} to {table_name}")
                connection.commit()

    except Exception as e:
        print(f"Error adjusting table: {e}")
        raise

# Endpoint nhận dữ liệu JSON hoặc file XLSX
@app.post("/api-one-task")
async def ingest_data(request: Request, file: UploadFile = File(None)):
    connection = get_db_connection()
    if connection is None:
        return {"error": "Database connection failed"}

    try:
        # Trường hợp nhận file XLSX
        if file:
            # Đọc file XLSX
            df = pd.read_excel(file.file)
            table_name = "data_table"
            
            # Kiểm tra và điều chỉnh bảng
            adjust_table_if_needed(connection, table_name, df)
            
            # Lấy danh sách cột từ file XLSX
            columns = df.columns.tolist()
            # Tạo câu lệnh INSERT động
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join(columns)
            insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            
            with connection.cursor() as cursor:
                for _, row in df.iterrows():
                    values = []
                    for col in columns:
                        value = str(row.get(col, ''))  # Dùng giá trị rỗng nếu cột không tồn tại trong hàng
                        values.append(value)
                    cursor.execute(insert_query, tuple(values))
                connection.commit()
            return {"status": "success", "message": "Data ingested from XLSX"}

        # Trường hợp nhận dữ liệu JSON
        else:
            data = await request.json()
            table_name = "data_table"
            
            # Kiểm tra và tạo bảng mặc định cho JSON
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    );
                """, (table_name,))
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    cursor.execute("""
                        CREATE TABLE data_table (
                            id SERIAL PRIMARY KEY,
                            field1 VARCHAR(255),
                            field2 VARCHAR(255),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    connection.commit()
                    print("Table data_table created successfully")
                
                cursor.execute(
                    "INSERT INTO data_table (field1, field2) VALUES (%s, %s)",
                    (data["field1"], data["field2"])
                )
                connection.commit()
            return {"status": "success", "message": "Data ingested from JSON"}

    except Exception as e:
        return {"error": str(e)}
    finally:
        connection.close()

# Chạy server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
