from fastapi import FastAPI


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

sql = "select count (order_id) from projeto_fia.ifood_distinct"

df = pd.read_sql(sql, engine)

app = FastAPI()

@app.get('/')
def root():
    return {str(df)}