import pandas as pd
from sqlalchemy import create_engine
from config import *
from datetime import datetime

conn = create_engine(
    url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
        DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
    )
)

def validate_date_format(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y.%m.%d")
        return True
    except ValueError:
        return False

def extract_tickmatch(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    query = """
        SELECT 
            st.Symbol,
            st.type,
            st.vol
            st.Time
        FROM stocksm_tickmatchs st
    """
    conditions = []
    params = []

    if start_date:
        conditions.append("`Time` >= %s")
        params.append(f"{start_date} 00:00:00")
    
    if end_date:
        conditions.append("`Time` <= %s")
        params.append(f"{end_date} 23:59:59")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    try:
        result = pd.read_sql(query, conn, params=params)
        return result
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)