import pandas as pd
from sqlalchemy import create_engine
import boto3
import numpy as np 
import fastparquet
from fastparquet import ParquetFile
import io


cred_dict = {
    'username': 'user',
    'pswd': 'user123',
    'port': 5432,
    'db_name': 'postgres'
}


secrets = {
    'aws_key':aws_access_key_id,
    'aws_secret':aws_secret_access_key,
}


def pd_df_to_postgresql(
    dataframe: pd.DataFrame, 
    schema_name: str, 
    table_name: str, 
    cred_dict: dict, 
    write_method: str ='fail', 
    **kwargs
) -> None:

    engine = create_engine(f'postgresql://{cred_dict["username"]}:{cred_dict["pswd"]}@192.168.0.1:{cred_dict["port"]}/{cred_dict["db_name"]}')
    print(engine)
    dataframe.to_sql(table_name, schema=schema_name, if_exists=write_method, index=False, con=engine, **kwargs)
    return None

def list_s3_bucket_files(
    bucket: str,
    aws_key: str,
    aws_secret: str,
    **kwargs,
)->list:
    
    list_of_files = []

    session = boto3.Session(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, **kwargs)

    s3 = session.resource('s3')
    
    my_bucket = s3.Bucket(bucket)

    for my_bucket_object in my_bucket.objects.all():
        list_of_files.append(my_bucket_object.key)

    return list_of_files

def s3_to_df(
    bucket: str,
    path: str,
    file_type: str,
    aws_key: str,
    aws_secret: str,
    **kwargs,
) -> pd.DataFrame:
  
    # s3_client = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, **kwargs)
    session = boto3.Session(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, **kwargs)
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket)
    obj = bucket.Object(path).get()
    data = io.BytesIO(obj['Body'].read())
    # response = s3_client.get_object(Bucket=bucket, Key=path)
    
    if file_type == 'csv':
        pass
        df = pd.read_csv(data, keep_default_na=False, na_values={""}, dtype=str)
    elif file_type == 'parquet':
        df = pd.read_parquet(data, engine='pyarrow')

    return df

def create_engine_postgres(
    cred_dict: dict, 
    **kwargs
) -> None:

    engine = f'postgresql://{cred_dict["username"]}:{cred_dict["pswd"]}@192.168.0.1:{cred_dict["port"]}/{cred_dict["db_name"]}'
    print(engine)
    # dataframe.to_sql(table_name, schema=schema_name, if_exists=write_method, index=False, con=engine, **kwargs)
    return str(engine)


bucket = 'alunos-monstros-iot'


list_of_files = list_s3_bucket_files(
    bucket, 
    secrets['aws_key'], 
    secrets['aws_secret']
    )

df = s3_to_df(bucket, list_of_files[0], 'parquet', secrets['aws_key'], secrets['aws_secret'])

engine = create_engine_postgres(cred_dict)

print(engine)

df.to_sql('ifood_distinct', con=engine, schema='public', if_exists='replace')

