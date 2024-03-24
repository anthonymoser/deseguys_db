import sqlite3 
import pandas as pd 

conn = sqlite3.connect('campaigns/il_campaign_disclosure.db')
cur = conn.cursor() 

tables = ['candidates', 'committees', 'receipts']
for t in tables: 
    print(f"extracting {t}")
    pd.read_sql(f"SELECT * FROM {t}", conn).to_csv(f"{t}.csv", index=False)