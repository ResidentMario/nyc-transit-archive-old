import datetime as dt
import os
import string
import urllib.request
from urllib.error import HTTPError, URLError
import time

import boto3
from botocore.client import Config

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


FEEDLIST = [
    'http://datamine.mta.info/mta_esi.php?feed_id=1',
    'http://datamine.mta.info/mta_esi.php?feed_id=11',
    'http://datamine.mta.info/mta_esi.php?feed_id=16',
    'http://datamine.mta.info/mta_esi.php?feed_id=2',
    'http://datamine.mta.info/mta_esi.php?feed_id=21',
    'http://datamine.mta.info/mta_esi.php?feed_id=26',
    'http://datamine.mta.info/mta_esi.php?feed_id=31',
    'http://datamine.mta.info/mta_esi.php?feed_id=36',
    'http://datamine.mta.info/mta_esi.php?feed_id=51'
]
DATAMINE_CONNECTION_TIMEOUT = 10
S3_TIMEOUT = 10
# s3://quilt-transit-archive is the production bucket; for testing use s3://aleksey-t4-test
# s3 = boto3.resource('s3').Bucket('aleksey-t4-test')
s3_config = Config(connect_timeout=S3_TIMEOUT, read_timeout=S3_TIMEOUT)
s3 = boto3.resource('s3', config=s3_config).Bucket('quilt-transit-archive')

def fetch_feed(**context):
    feed_url = context['params']['feed_url']
    feed_id = context['params']['feed_id']
    mtakey = os.environ['mtakey']
    url = f"{feed_url}&key={mtakey}"

    timestamp = dt.datetime.utcnow()
    f_time = timestamp.strftime('%Y%m%dT%H%MZ')  # e.g. '20190503T0932Z'
    try:
        response = urllib.request.urlopen(url, timeout=DATAMINE_CONNECTION_TIMEOUT)
    except (HTTPError, URLError):
        raise OSError(f'{url} took longer than {DATAMINE_CONNECTION_TIMEOUT}s to respond.')

    data = response.read()

    if data == b"Permission denied" or response.getcode() == 404:
        # the server is temporarily unavailable, there was a network error, or perhaps we tried
        # to read from the feed during the the non-atomic write process; if we retry later we are
        # likely to succeed
        time.sleep(5)

        try:
            response = urllib.request.urlopen(url, timeout=DATAMINE_CONNECTION_TIMEOUT)
        except (HTTPError, URLError):
            raise OSError(f'{url} took longer than {DATAMINE_CONNECTION_TIMEOUT}s to respond.')

        data = response.read()

        if data == b"Permission denied" or response.getcode() == 404:
            # the feed seems to be down
            raise ValueError(f'Could not successfully get a response from "{feed_url}".')

    filename = f'raw/{feed_id}/{f_time}.pb'
    s3.put_object(Body=data, Key=filename)


with airflow.DAG(
    'archive_gtfs_rt_to_s3', 
    default_args={
        'owner': 'airflow',
        'start_date': dt.datetime(2019, 5, 3),
        'concurrency': 1,
        'retries': 0
    },
    schedule_interval='*/1 * * * *',
    catchup=False
) as dag:
    for feed_url in FEEDLIST:
        substr = '?feed_id='
        feed_id = int(feed_url[feed_url.rfind(substr) + len(substr):])
        task_id = f'fetch_feed_{feed_id}'
        task = PythonOperator(
            task_id=task_id,
            params={'feed_url': feed_url, 'feed_id': feed_id},
            python_callable=fetch_feed,
            provide_context=True
        )
        dag.add_task(task)
