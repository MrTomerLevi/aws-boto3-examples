#!/usr/bin/env python3

import boto3
import time
from datetime import datetime, timezone, timedelta

class MonitoredTable:
    def __init__(self, table_name, ts_column_name):
        self.table_name = table_name
        self.ts_column_name = ts_column_name

monitored_tables = [
    MonitoredTable('users', 'creation_timestamp'),
    MonitoredTable('orders', 'order_timestamp')
]

database = 'company_db_demo'
output_location = 's3://aws-athena-query-results-*******/'
query = """SELECT {} FROM {} order by {} desc limit 1"""

boto3.setup_default_session()
client = boto3.client('athena',region_name='eu-west-2')

def main():  
    for tbl in monitored_tables:
        fmt_query = query.format(tbl.ts_column_name ,tbl.table_name, tbl.ts_column_name)
        response = get_athena_results(fmt_query)

        latest_record_ts = response['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
        latest_record_datetime = datetime.strptime(latest_record_ts, '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)


def get_athena_results(fmt_query): 
    print("Executing query: {}".format(fmt_query))
    qry_execution_response = execute_athena_query(fmt_query)

    time.sleep(10)
    response = client.get_query_results(
        QueryExecutionId=qry_execution_response['QueryExecutionId'],
    )
    return response

def execute_athena_query(fmt_query):
    qry_execution_response = client.start_query_execution(
        QueryString = fmt_query,
        QueryExecutionContext = {
            'Database': database
        },
        ResultConfiguration = {
            'OutputLocation': output_location
        }
    )
    return qry_execution_response

if __name__ == '__main__':  
   main()



