from fastapi import FastAPI
import json


import pandas as pd
from sqlalchemy import create_engine

cred_dict = {
    'username': 'postgres',
    'pswd': 'postgres123',
    'port': 5432,
    'db_name': 'postgres',
    "endpoint": 'postgres.cb1mclnabjja.us-east-1.rds.amazonaws.com'
}


engine = create_engine(f'postgresql://{cred_dict["username"]}:{cred_dict["pswd"]}@{cred_dict["endpoint"]}:{cred_dict["port"]}/{cred_dict["db_name"]}')

sql = "select * from projeto_fia.ifood_distinct limit 10"

df = pd.read_sql(sql, engine)

for item in df.columns:

    df[item] = df[item].astype('str')
    
df = df.to_dict()

app = FastAPI()

@app.get("/{base}/{col_id}")
async def read_col(base, col_id):

    if base == 'ifood_distinct':

        sql = "select * from projeto_fia.ifood_distinct limit 10"

        df = pd.read_sql(sql, engine)

        for item in df.columns:

            df[item] = df[item].astype('str')
            
        df = df.to_dict()

        return {str(df[col_id])}
    
    else:

        sql = "select * from projeto_fia.resultado_modelo"

        df = pd.read_sql(sql, engine)

        for item in df.columns:

            df[item] = df[item].astype('str')
            
        df = df.to_dict()

        return {str(df[col_id])}
        