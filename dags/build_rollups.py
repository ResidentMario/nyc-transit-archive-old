from datetime import datetime, timedelta
from dateutil import tz
import os
import json
import itertools

import boto3
from botocore.client import Config
import airflow
from airflow.operators.python_operator import PythonOperator
import gtfs_tripify as gt


AWS_BUCKET_NAME = 'quilt-transit-archive'
FEEDMAP = {
    1:  ['1', '2', '3', '4', '5', '6', 'GS'],
    2:  ['L'],
    11: ['SI'],
    16: ['N', 'Q', 'R', 'W'],
    21: ['B', 'D', 'F', 'M'],
    26: ['A', 'C', 'E', 'H', 'FS'],
    31: ['G'],
    36: ['J', 'Z'],
    51: ['7']
}
ROUTE_IDS = itertools.chain(*FEEDMAP.values())
S3_TIMEOUT = 60

s3_config = Config(connect_timeout=S3_TIMEOUT, read_timeout=S3_TIMEOUT)
aws_client = boto3.client('s3', config=s3_config)

# Rollups cover a 12am-12pm window, e.g. an entire day in EST time. Note that this interval is
# in New York time and it may be longer or shorter than a true 24 hour period due to daylight 
# savings time, leap seconds, and other similar complications. 
# 
# Timezone aware run schedules such as this are implemented in Airflow using a timezone-aware 
# start date and a timedelta. More info: https://airflow.apache.org/timezone.html#time-deltas.
#
# Rollups are flush left and ragged right: they contain every trip that started in the 12am-12pm
# window, but there are trips that start in that window that don't also end in it. To account 
# for this, processing is done once daily at 3am local time, e.g. with a 3 hour "bonus window".
# Trips that start before 12am and end before 3am are extracted from this subset; trips that 
# start in this 12am-3am bonus window are also processed, but discarded. This is computationally
# inefficient but simple code-wise, which is why we do it this way!
# 
# One last thing to note. The 11:59pm message from the previous time window is also read in. This
# is done because if a trip is in the 12pm update, the only way to know for sure whether it 
# started at that time or at an earlier time is to check the previous message to see if it 
# showed up there too.

# construct the local timestamps
new_york_tz = tz.gettz('America/New_York')
utc = tz.gettz('UTC')
today_est = datetime.now().astimezone(new_york_tz)
day_before_yesterday_est = today_est - timedelta(days=2)
today_est_year, today_est_month, today_est_day =\
    today_est.year, today_est.month, today_est.day
day_before_yesterday_year, day_before_yesterday_month, day_before_yesterday_day =\
    day_before_yesterday_est.year, day_before_yesterday_est.month, day_before_yesterday_est.day

# construct the corresponding UTC timestamps (this will account for DST, etcetera)
today_utc_endpoint = datetime(
    today_est_year, today_est_month, today_est_day, 3, tzinfo=new_york_tz
).astimezone(utc)
day_before_yesterday_utc_startpoint = datetime(
    day_before_yesterday_year, day_before_yesterday_month, 
    day_before_yesterday_day, 23, 59, tzinfo=new_york_tz
).astimezone(utc)
rollup_start_time = day_before_yesterday_utc_startpoint.strftime("%Y%m%d%H")

# iterate (in UTC, which is safe) to get feed timestamps
time_iterator = day_before_yesterday_utc_startpoint
ts_f_strings = []
while time_iterator <= today_utc_endpoint:
    ts_f_strings.append(time_iterator.strftime('%Y%m%dT%H%MZ'))
    time_iterator = time_iterator + timedelta(minutes=1)


def parse_feed(**context):
    feed_id = context['params']['feed_id']
    ts_f_strings = context['params']['ts_f_strings']
    parse_errors = []

    filenames = [f'raw/{feed_id}/{ts_f_string}.pb' for ts_f_string in ts_f_strings]

    # we could process everything in one go, but memory usage would be extremely high
    # gtfs_tripify defines a merge operation, which we take advantage of with batching
    chunk_size = 60 * 4  # up to 4hrs
    segments = []
    for chunk_idx in range(0, len(filenames), chunk_size):
        filenames_slice = filenames[chunk_idx:chunk_idx + chunk_size]
        stream = []

        for filename in filenames_slice:
            try:
                update = aws_client.get_object(Bucket=AWS_BUCKET_NAME, Key=filename)['Body']
                stream.append(update.read())
            except aws_client.exceptions.NoSuchKey:
                # not all updates are actually written; there may be issues either with the
                # DAG scheduler or issues downstream with the data provider
                parse_errors.append({
                    'type': 'feed_update_file_not_available',
                    'details': {
                        'update_filepath': filename,
                        'update_timestamp': time_iterator.timestamp()
                    }
                })

        logbook, timestamps, new_parse_errors = gt.logify(stream)
        parse_errors += new_parse_errors
        segments.append((logbook, timestamps))

    logbook, timestamps = gt.ops.merge_logbooks(segments)
    logbook = gt.ops.cut_cancellations(logbook)
    logbook = gt.ops.discard_partial_logs(logbook)
    logbooks, _ = gt.ops.partition_on_route_id(logbook, timestamps)

    for route_id in logbooks:
        if route_id not in ROUTE_IDS:
            parse_errors.append({
                'type': 'feed_update_included_unknown_route_id',
                'details': {'route_id': route_id}
            })
            continue

        gt.ops.to_csv(logbooks[route_id], f'stop_times_{route_id}.csv')
        aws_client.upload_file(
            Filename=f'stop_times_{route_id}.csv',
            Bucket=AWS_BUCKET_NAME,
            Key=f'rollups/{rollup_start_time}/{route_id}/stop_times.csv'
        )

    for route_id in logbooks:
        os.remove(f'stop_times_{route_id}.csv')

    with open('parse_errors.json', 'w') as fp:
        json.dump(parse_errors, fp, indent=4)
    aws_client.upload_file(
        Filename='parse_errors.json',
        Bucket=AWS_BUCKET_NAME,
        Key=f'rollup_logs/{rollup_start_time}/parse_errors.json'
    )
    os.remove('parse_errors.json')


# TODO: are these scheduling shenanigans necessary on a local instance?
# The DAG must run once daily after all of the data in the window of interest is available
# and before the next day has started. E.g. if we want the roll-up for 1 January (in EST), we 
# need to run sometime before end of day 2 January (in UTC), but not before 3am (in EST).
#
# Ideally this would be datetime(2019, 5, 7, 3, tzinfo=new_york_tz) and timedelta(hours=24).
# However, Cloud Composer does not appear to support timezone-aware Airflow DAG schedules:
# https://stackoverflow.com/q/56081480/1993206.
#
# So instead we schedule to run at 12 noon UTC. This is "good enough" because 
# EST ∈ {UTC-4, UTC-5} so 12 noon UTC is both always after 3am EST and but also always on the
# same day. This is a rather unfortunate hack, though.
with airflow.DAG(
    'build_daily_rollups',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2019, 5, 7, 12),
        'concurrency': 1,
        'retries': 1  # safe b/c this DAG is idempotent
    },
    schedule_interval=timedelta(hours=24),
    catchup=False
) as dag:
    roll_up_tasks = []
    for feed_id in FEEDMAP:
        task_id = f'roll_up_feed_{feed_id}'
        task = PythonOperator(
            task_id=task_id,
            params={'feed_id': feed_id, 'ts_f_strings': ts_f_strings},
            python_callable=parse_feed,
            provide_context=True
        )
        dag.add_task(task)
        roll_up_tasks.append(task)
