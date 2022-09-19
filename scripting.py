from sqlalchemy import create_engine
import psycopg2
import io
import pandas as pd


df = pd.read_csv('x60.csv')
print(df.head())
engine = create_engine('postgresql+psycopg2://postgres:Emoz3832!@localhost:5432/VIVINO')

df.head(0).to_sql('table_name', engine, if_exists='replace',index=False) #drops old table and creates new empty table

conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, 'table_name', null="") # null values become ''
conn.commit()





