import json
import datetime
import uuid
import botocore
import time

import pandas as pd

def athena_query(client, DB_NAME, query, output):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DB_NAME},
        ResultConfiguration={'OutputLocation': output}
    )
    return response

def cleanup(s3_client, BUCKET_NAME, folder):
    folder = '/'.join(folder.split('/')[3:])
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder)

    print("Eliminando..." )
    for object in response['Contents']:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=object['Key'])
    print("Eliminacion Terminada")
    
def execute_query(s3_client, client, query_start, BUCKET_NAME, output):
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
        cleanup(s3_client, BUCKET_NAME, output)
        return result
    except Exception as e:
        print(e)  

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

def file_names(s3, BUCKET_NAME, s):
    if len(s.split(',')) == 5:
        today = datetime.datetime.today()
        year = today.strftime('%Y')
        month = today.strftime('%m').zfill(2)
        day = today.strftime('%d').zfill(2)
    elif len(s.split(',')) == 6:
        today = s.split(',')[0]
        year = today.split('-')[0]
        month = today.split('-')[1].zfill(2)
        day = today.split('-')[2].zfill(2)
    else:
        raise Exception

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

def _is_valid_s3_path(s3, BUCKET_NAME, path):
    bucket = s3.Bucket(BUCKET_NAME)
    return sum(1 for _ in bucket.objects.filter(Prefix=path)) > 0

def create_partition(s3, client, DB_NAME, BUCKET_NAME, TABLE_NAME, key_route):
    date_params = key_route.split('/')[-4:-1]
    year, month, day = [int(params.split('=')[-1]) for params in date_params]
    query = f"ALTER TABLE {TABLE_NAME} ADD IF NOT EXISTS \
    PARTITION (YEAR={year},MONTH={month},DAY={day}) LOCATION 's3://{BUCKET_NAME}/{key_route}'"
    output =  f"s3://{BUCKET_NAME}/temp/"
    print("Creating partition")
    query_start = athena_query(client, DB_NAME, query, output)
    execute_query(s3, client, query_start, BUCKET_NAME, output)

def save_info(s3, client, bot, DB_NAME, BUCKET_NAME, TABLE_NAME, key, data, recipient):
    key_route = '/'.join(key.split('/')[:-1]) + '/'
    route_exists = _is_valid_s3_path(s3, BUCKET_NAME, key_route)
    s3object = s3.Object(bucket_name=BUCKET_NAME,key=key)
    s3object.put(Body=(bytes(json.dumps(data).encode('UTF-8'))))
    if route_exists == False:
        create_partition(s3, client, DB_NAME, BUCKET_NAME, TABLE_NAME, key_route)
        bot.send_message(recipient, "Partition created correctly")

    bot.send_message(recipient, "Data inserted correctly")

def extract_today_info(s3_client, client, BUCKET_NAME, DB_NAME, TABLE_NAME):
    today = datetime.datetime.today()
    year = today.strftime('%Y')
    month = today.strftime('%m')
    day = today.strftime('%d')
    
    query = f"""
        SELECT * FROM {TABLE_NAME}
        WHERE YEAR = {year} AND MONTH = {month} AND DAY = {day}
        """
        
    output = f"s3://{BUCKET_NAME}/temp/"
    query_start = athena_query(client,DB_NAME,query,output)

    return execute_query(s3_client, client, query_start, BUCKET_NAME, output)

def response_to_df(results):
    df = [[data.get('VarCharValue') for data in row['Data']] for row in results['ResultSet']['Rows']]
    df = pd.DataFrame(df[1:], columns=df[0])
    df['ammount'] = df.ammount_cents.astype(float) / 100
    df = df[['ammount','category','card','detail','relation']]
    return df