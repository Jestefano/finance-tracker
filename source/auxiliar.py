import json
import datetime
import uuid
import botocore
import time

import pandas as pd

def preprocessing_info(data):
    if('.' in data['ammount_cents']):
        if(len(data['ammount_cents'].split('.')[1]) == 1): 
            data['ammount_cents'] = data['ammount_cents'] + '0'
        data['ammount_cents'] = int(data['ammount_cents'].replace('.',''))
    else:
        data['ammount_cents'] = int(data['ammount_cents']) * 100
    
    # Preprocessing of relation
    if(data['relation'].isdigit()):
        data['relation'] = int(data['relation'])
    else:
        data['relation'] = None

    return data

def file_names(s3, BUCKET_NAME, s = None):
    if s is None:
        today = datetime.datetime.today()
        year = today.strftime('%Y')
        month = today.strftime('%m')
        day = today.strftime('%d')
    else:
        today = s.split(',')[0]
        year = today.split('-')[0]
        month = today.split('-')[1]
        day = today.split('-')[2]

    while True:
        id_ = uuid.uuid1()
        key = f'data/YEAR={year}/MONTH={month}/DAY={day}/{id_.hex}.json'
        try:
            s3.Object(BUCKET_NAME, key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                break

    return key

def generate_json(s):
    values = s.split(',')
    if(len(values) == 6):
        values = values[1:] # Eliminar fecha, ya se uso
    key_names = ['ammount_cents','category','card','detail','relation']
    data = {i:j for i,j in zip(key_names,values)}
        
    return data

def save_info(s3,bot,BUCKET_NAME, key, data, recipient):
    s3object = s3.Object(bucket_name=BUCKET_NAME,key=key)
    s3object.put(
        Body=(bytes(json.dumps(data).encode('UTF-8')))
    )    
    bot.send_message(recipient, "Data inserted correctly")

def extract_today_info(client, BUCKET_NAME, DB_NAME, TABLE_NAME):
    today = datetime.datetime.today()
    year = today.strftime('%Y')
    month = today.strftime('%m')
    day = today.strftime('%d')
    
    query = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE YEAR = {year}
        AND MONTH = {month}
        AND DAY = {day}
        """
    output = f"s3://{BUCKET_NAME}/temporal_query"
    
    query_start = client.start_query_execution(
        QueryString=query, 
        QueryExecutionContext={'Database':DB_NAME},
        ResultConfiguration = { 'OutputLocation': output})
    # Way to handle queries in athena
    try:              
        query_status = None
        while 1:
            query_status = client.get_query_execution(QueryExecutionId=query_start["QueryExecutionId"])['QueryExecution']['Status']['State']
            if query_status == 'FAILED' or query_status == 'CANCELLED':
                raise Exception('Athena query failed or was cancelled')
            elif(query_status == 'QUEUED' or query_status == 'RUNNING'):
                time.sleep(2)
            else: 
                result = client.get_query_results(QueryExecutionId=query_start['QueryExecutionId'])
                break
        return result
    except Exception as e:
        print(e)     

def response_to_df(results):
    df = [[data.get('VarCharValue') for data in row['Data']] for row in results['ResultSet']['Rows']]
    df = pd.DataFrame(df[1:], columns=df[0])
    df['ammount'] = df.ammount_cents.astype(float) / 100
    df = df[['ammount','category','card','detail','relation']]
    return df