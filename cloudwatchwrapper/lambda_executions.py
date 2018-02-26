"""
Library function for getting the number of concurrent executions.
"""

import boto3
import botocore
from datetime import datetime, timedelta, timezone


def get_lambda_execs_in_last_min():
    """
    Use CloudWatch to see how many lambdas are currently running (take the
    average over the last minute).

    Returns:
        (int): Average number of currently running lambdas.
    """

    utc_zone = timezone(timedelta(hours=0))
    now = datetime.now()
    delta = timedelta(minutes=-1)

    # Assume no concurrent executions if no data found.
    num_execs = 0

    cw = boto3.client('cloudwatch')

    try:
        resp = cw.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='ConcurrentExecutions',
            #Dimensions=[],
            StartTime=now + delta,
            EndTime=now,
            Period=60,
            Statistics=['Average']
        )
    except botocore.ClientError as ex:
        print(ex)
        return num_execs
    
    if 'Datapoints' in resp and len(resp['Datapoints']) > 0:
        if 'Average' in resp['Datapoints'][0]:
            num_execs = resp['Datapoints'][0]['Average']

    return num_execs

