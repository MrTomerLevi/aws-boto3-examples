#!/usr/bin/env python3

import boto3
from datetime import datetime, timedelta

client = boto3.client('logs')

# the name of the reuired LogGroup (find it in AWS Console -> CloudWatch -> logs)
log_group_name = '/aws/lambda/copd-s3-monitor'
# number of hours from now back to filter messages in
log_history_hours = -10

def filter_logs(log_group_name, start_time_ms, end_time_ms):
    response = client.filter_log_events(
    logGroupName=log_group_name,
    filterPattern='Error',
    startTime=start_time_ms,
    endTime=end_time_ms,
    limit=100)

    return response

def compose_timestamp_to_error_str(events):
    s = ""
    for msg in events:
        message_time = datetime.fromtimestamp(msg['timestamp'] / 1000)
        s += "timestamp: {} , message: {}\n".format(message_time,msg['message'])
    return s

def main():
    
    now_date = datetime.now()
    now_ms = int(now_date.strftime('%s')) * 1000
    start_time_ms = int((now_date + timedelta(hours=log_history_hours)).strftime('%s')) * 1000

    logs = filter_logs(log_group_name, start_time_ms, now_ms)
    print(compose_timestamp_to_error_str(logs['events']))

if __name__ == '__main__':
    main()
