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
        
        
def extract_symbols() -> pd.DataFrame:
    query = """
        select 
            ss.SymbolName ,
            ss.SectorName 
        from sm_stocks ss 
    """
    try:
        result = pd.read_sql(query, conn)
        return result
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)

def clean_tickmatch_df(tickmatch_df):
    tickmatch_df['Date'] = tickmatch_df['Time'].str.split(' ',expand=True)[0]
    tickmatch_df.loc[:,'Date'] = pd.to_datetime(tickmatch_df['Date'], format="%Y.%m.%d")
    tickmatch_df.loc[tickmatch_df['Type'] == 'BUY', "Sign"] = 1
    tickmatch_df.loc[tickmatch_df['Type'] == 'SELL', "Sign"] = -1
    tickmatch_df = tickmatch_df[~tickmatch_df['Sign'].isna()]
    tickmatch_df.loc[:,"Values"] = tickmatch_df['Vol'] * tickmatch_df['Sign'] * tickmatch_df['Last']
    return tickmatch_df[['Symbol','Values','Date']]

def agg_accum_sector(tickmath_df,symbol_df):
    values_df = tickmath_df.groupby(by=['Date','Symbol']).sum()
    values_df = values_df.reset_index()
    merge_df = values_df.merge(symbol_df, left_on='Symbol',right_on='SymbolName').drop(columns=['SymbolName'])
    merge_df = merge_df.fillna(value={'SectorName':'Other'})
    merge_df = merge_df[['Date','SectorName','Values']]
    accum_sector_df=merge_df.groupby(by=['SectorName','Date']).sum().reset_index()
    return accum_sector_df
