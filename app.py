from flask import Flask,request
from service.sector_rotation import *

app = Flask(__name__)

@app.route('/sector_rotation')
def sector_rotation():
    start_date = request.args.get('s',default=None)
    end_date = request.args.get('e',default=None)
    tickmatch_df = extract_tickmatch(start_date,end_date)
    symbol_df = extract_symbols()
    tickmatch_df = clean_tickmatch_df(tickmatch_df)
    print(tickmatch_df)
    sector_df = agg_accum_sector(symbol_df,tickmatch_df)
    return sector_df.to_dict()