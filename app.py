from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.config import Config
import uvicorn
from os import getenv, urandom, path, environ

import json
import botocore
import boto3
import random
import sys
import time
from random import choice
from string import ascii_uppercase

boto3.set_stream_logger(name='botocore')
region = "us-east-1"
client = boto3.client("timestream-write", region_name= region)


def create_database(db):
    try:
        client.create_database(DatabaseName=db)
    except :
        print(db + " create failed")
        raise
            
def create_tables(db, prefix, start, n):
    for i in range(start, n) :
        TABLE_NAME = prefix + str(i)
        print(TABLE_NAME)
        try:
            client.create_table(DatabaseName=db, TableName=TABLE_NAME)
        except :
            print(TABLE_NAME + " create failed")
            raise
    

def ingest_data_to_table(db, table,  n, record_count):
 
    for i in range(0, n) :
        #print("Ingesting to Table:", table)
        
        for i in range(0, record_count):
            write_records_with_common_attributes(db, table, i)
            
def ingest_data(db, prefix, start, n, record_count):
    TABLE_NAME = prefix 

    for i in range(start, n) :
        TABLE_NAME = prefix + str(i)
        print("Ingesting to Table:", TABLE_NAME)
        
        for i in range(0, record_count):
            write_records_with_common_attributes(db, TABLE_NAME, i)
        
def _current_milli_time():
    return str(int(round(time.time() * 1000)))
        
def write_records_with_common_attributes(db,table,record_count):
    current_time = _current_milli_time()

    dimensions = [
        {'Name': 'region', 'Value': 'us-east-1'},
        {'Name': 'az', 'Value': 'az1'},
        {'Name': 'hostname', 'Value': 'host1'}
    ]

    common_attributes = {
        'Dimensions': dimensions,
        'MeasureValueType': 'DOUBLE',
        'Time': current_time
    }

    cpu_utilization = {
        'MeasureName': 'cpu_utilization',
        'MeasureValue': '13.5'
    }

    memory_utilization = {
        'MeasureName': 'memory_utilization',
        'MeasureValue': '40'
    }

    record_counter = {
        'MeasureName': 'record_counter',
        'MeasureValue': str(record_count)
    }

    records = [cpu_utilization, memory_utilization, record_counter]

    try:
        result = client.write_records(DatabaseName=db, TableName=table,
                                           Records=records, CommonAttributes=common_attributes)
        print("[%s][%s] WriteRecords Status: [%s]" % (db, table, result['ResponseMetadata']['HTTPStatusCode']))
    except client.exceptions.RejectedRecordsException as err:
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        #print("Other records were written successfully. ")
    except Exception as err:
        print("Error:", err)
        raise
        
       
async def homepage(request):
    suffix = ''.join(choice(ascii_uppercase) for i in range(60))
    
    DATABASE_NAME = "GB1_" + suffix
    TABLE_NAME = "GB_" + suffix
    
    create_database(DATABASE_NAME)
    create_tables(DATABASE_NAME,TABLE_NAME,0, 1 )
    ingest_data(DATABASE_NAME,TABLE_NAME,0,1,100 )
    
    return JSONResponse({'statusCode': 200, 'DATABASE_NAME': DATABASE_NAME,'TABLE_NAME': TABLE_NAME})


app = Starlette(debug=True, routes=[
    Route('/', homepage),
])


config = Config()

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0",
                port=int(getenv('PORT', 8000)),
                log_level=getenv('LOG_LEVEL', "info"),
                debug=getenv('DEBUG', False),
                proxy_headers=True)
