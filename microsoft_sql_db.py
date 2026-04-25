

import pyodbc

def get_connection():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=salesforce_app;"
        "Trusted_Connection=yes;"
    )
    return conn


# Save Mapping
def save_mapping(object_name, mapping_dict):
    conn = get_connection()
    cursor = conn.cursor()

    for csv_col, sf_field in mapping_dict.items():
        cursor.execute("""
            MERGE mappings AS target
            USING (SELECT ? AS object_name, ? AS csv_column) AS source
            ON target.object_name = source.object_name 
               AND target.csv_column = source.csv_column
            WHEN MATCHED THEN
                UPDATE SET sf_field = ?
            WHEN NOT MATCHED THEN
                INSERT (object_name, csv_column, sf_field)
                VALUES (?, ?, ?);
        """, (object_name, csv_col, sf_field, object_name, csv_col, sf_field))

    conn.commit()
    conn.close()


# Load Mapping
def load_mapping(object_name):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT csv_column, sf_field FROM mappings WHERE object_name = ?",
        object_name
    )

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return {row[0]: row[1] for row in rows}


# Save History
def save_upload_history(file_name, object_name, success, failed):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO uploads (file_name, object_name, success_count, failed_count) VALUES (?, ?, ?, ?)",
        file_name, object_name, success, failed
    )

    conn.commit()
    cursor.close()
    conn.close()


# Get History
def get_upload_history():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM uploads ORDER BY created_at DESC")

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [dict(zip(columns, row)) for row in rows]

# Save User
def save_user(username, client_id, client_secret, token_url):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO users (username, client_id, client_secret, token_url)
        VALUES (?, ?, ?, ?)
    """, (username, client_id, client_secret, token_url))

    conn.commit()
    cursor.close()
    conn.close()


# Get User
def get_user(username):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()

    if row:
        columns = [col[0] for col in cursor.description]
        user = dict(zip(columns, row))
    else:
        user = None

    cursor.close()
    conn.close()

    return user

# Download Data 
def save_downloaded_data(object_name, df):
    conn = get_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO downloaded_data (object_name, data) VALUES (?, ?)",
            (object_name, str(row.to_dict()))
        )

    conn.commit()
    cursor.close()
    conn.close()
