

import pyodbc

def get_connection():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=salesforce_app;"
        "Trusted_Connection=yes;"
    )
    return conn

def save_mapping(*args,**kwargs):
    pass

def load_mapping(*args,**kwargs):
    return {}

def save_upload_history(*args,**kwargs):
    pass

def get_upload_istory(*args,**kwargs):
    return None

def save_user(*args,**kwargs):
    pass

def get_user(*args,**kwargs):
    return None

def save_downloaded_data(*args,**kwargs):
    pass
