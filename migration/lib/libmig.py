import sys
import os
import time
import logging
import psycopg2
import boto3
import botocore
import getpass
import datetime
import random
import math

from multiprocessing import Process, Manager, Queue

from lib.config import *


# we obtain the logger declared in main for use within this module
logger = logging.getLogger("miglogger")


def get_random_string(length, charset):

    return ''.join(random.choice(charset) for i in range(length))


# this function generates a convenient file name for a log file
# it includes the username and date for traceability and portability
# it includes a random component to prevent accidental overwriting
def get_log_filename():

    user_str   = getpass.getuser()
    date_str   = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    random_str = get_random_string(6, CHARSET_TMP)

    return f"{LOG_DIR}/sketch_migrate_{user_str}_{date_str}_{random_str}.log"


# this function generates a random key name
def get_random_keyname():

    user_str     = getpass.getuser()
    random_str_1 = get_random_string(24, CHARSET_TMP)
    random_str_2 = get_random_string(24, CHARSET_TMP)

    # this file certainly does not exist :-)
    # the last component is just for the unlikely case that there is a problem deleting yet
    # so that we know where it came from
    return f"{random_str_1}/{random_str_2}.sketch_migration_{user_str}"


# this function obtains a database connection
def get_db_connection():

    return psycopg2.connect(DB_CONN_STRING)


# this function obtains an S3 connection
def get_s3_connection():

    session = boto3.session.Session()
    s3_connection = session.client('s3',
                                   config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
                                   region_name=AWS_DEFAULT_REGION,
                                   endpoint_url=S3_ENDPOINT_URL_LEG,
                                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    return s3_connection


# this function checks the current S3 and database status
def check_s3_status(s3_connection, bucket_name):

    response = s3_connection.list_objects_v2(Bucket=bucket_name, MaxKeys=S3_MAX_OBJECTS_REQ)

    try:
        objects = response['Contents']
        nr_found_objects_partial = len(objects)
        nr_found_objects_total   = len(objects)
    except Exception as e:
        nr_found_objects_partial = 0
        nr_found_objects_total   = 0

    # we need to loop because the list_objects_v2 functions never returns more than 1000
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html#list-objects-v2

    while nr_found_objects_partial > 0:

        if not response.get('IsTruncated'):
            break
        else:
            continuation_token = response.get('NextContinuationToken')

        response = s3_connection.list_objects_v2(Bucket=bucket_name, MaxKeys=S3_MAX_OBJECTS_REQ, ContinuationToken=continuation_token)
        try:
            objects = response['Contents']
            nr_found_objects_partial = len(objects)
            nr_found_objects_total += nr_found_objects_partial
            logger.debug(f"  * found so far {nr_found_objects_total}, bucket {bucket_name} has more objects")
        except Exception as e:
            logger.debug(f"  * found so far {nr_found_objects_total} bucket {bucket_name} has NO more objects")
            nr_found_objects_partial = 0

    logger.info(f"  * {nr_found_objects_total} objects in bucket {bucket_name}")

    return nr_found_objects_total


# this functions summarizes the DB status
def check_db_status(db_connection):

    cur = db_connection.cursor()

    try:
        cur.execute('SELECT COUNT(*) from avatars WHERE path LIKE(\'image/%\');')
        legacy_count = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) from avatars WHERE path LIKE(\'avatar/%\');')
        prod_count = cur.fetchone()[0]

        cur.execute('SELECT COUNT(*) from avatars')
        total_count = cur.fetchone()[0]

        entry_diff = total_count - (legacy_count + prod_count)

        logger.info(f"  * {legacy_count} legacy entries in the database")
        logger.info(f"  * {prod_count} production entries in the database")
        logger.info(f"  * {entry_diff} unexpected entries in the database")
        logger.info(f"  * {total_count} total entries in the database")

    except Exception as e:
        logger.error(f"Error querying database for status: {e}")

    return [ legacy_count, prod_count, total_count ]


# this function summarizes the S3 status
def check_status(db_connection, s3_connection, request_confirmation):

    logger.info('')
    logger.info('Current data status:')
    nr_found_objects_legacy     = check_s3_status(s3_connection, S3_BUCKET_NAME_LEG)
    nr_found_objects_production = check_s3_status(s3_connection, S3_BUCKET_NAME)
    nr_found_objects_total = nr_found_objects_legacy + nr_found_objects_production

    s3_status_list = [ nr_found_objects_legacy, nr_found_objects_production ]

    logger.info(f"  * {nr_found_objects_total} total objects found\n")

    db_status_list = check_db_status(db_connection)

    # prepare a CSV string, as it is easier to parse from the outside
    status_list = s3_status_list + db_status_list
    status_str = ','.join(str(x) for x in status_list)

    logger.info('')
    logger.info('NOTE: this script does not delete legacy objects')

    if request_confirmation:
        user_response = input('\nAre you sure you want perform the migration over this data status? (yes/no) ')

        if user_response != 'yes':
            logger.info('')
            logger.info('Execution canceled')
            exit(E_ERR)
        else:
            logger.info('')
            return status_str

    return status_str


# this function checks if we have write permissions a bucket
def check_bucket_write_permissions(s3_connection, bucket_name):

    try:
        hello_world_key = get_random_keyname()
        s3_connection.put_object(Bucket=bucket_name, Key=hello_world_key, Body='hello world!')
        s3_connection.delete_object(Bucket=bucket_name, Key=hello_world_key)
    except Exception as e:
        logger.error(f"Error while creating an test s3 object on bucket {bucket_name}")
        logger.error('  * check domain name, bucket name, key/secret pair and bucket write permissions')
        exit(E_ERR)


# this function checks if we have read permissions on a production bucket
def check_bucket_read_permissions(s3_connection, bucket_name):

    try:
        s3_connection.list_objects_v2(Bucket=bucket_name,)
    except Exception as e:
        logger.error(f"Error while listing the contents of bucket {bucket_name}")
        logger.error('  * check domain name, bucket name, key/secret pair and bucket write permissions')
        exit(E_ERR)


# this function copies a batch of legacy files present on the legacy bucket to the production bucket
def copy_s3_batch(s3_connection, bucket_src, bucket_dst, batch, dry_run=False, overwrite=False):

    # because of process concurrency we need to delay the logs of this function
    # and log them all at once
    messages_to_log = []

    start_time = time.time()

    if dry_run:
        msg_prefix = 'DRY RUN '
    else:
        msg_prefix = ''

    messages_to_log.append('Got S3 batch')

    sucessfully_copied = []
    for row in batch:
        old_key = row[1]
        copy_source = { 'Bucket': bucket_src, 'Key': old_key }
        filename = os.path.basename(old_key)
        new_key = f"avatar/{filename}"

        if overwrite is True:
            skip = False
            messages_to_log.append(f"  * {msg_prefix}copying {bucket_src}/{old_key} to {bucket_dst}/{new_key}")
        else:
            # check first if an object with the same key is already in the production bucket
            # for performance, integrity and idempotency reasons we do not overwrite an existing file on bucket_dst
            response = s3_connection.list_objects_v2(Bucket=bucket_dst, Prefix=new_key)
            try:
                objects = response['Contents']
                skip = True
                messages_to_log.append(f"  * skipping {bucket_src}/{old_key} as {bucket_dst}/{new_key} already exists")
            except Exception as e:
                skip = False
                messages_to_log.append(f"  * {msg_prefix}copying {bucket_src}/{old_key} to {bucket_dst}/{new_key}")

        if skip is not True:
            try:
                # reference https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/copy.html#copy
                if not dry_run:
                    s3_connection.copy(copy_source, bucket_dst, new_key)
                # we store the list of sucessfully copied files
                sucessfully_copied.append(row)
            except Exception as e:
                messages_to_log.append(f"Error copying file {old_key}: {e}")

    messages_to_log.append('S3 batch done')

    end_time = time.time()

    elapsed_time = end_time - start_time
    avg_time_per_file = round(elapsed_time / len(batch), 2)  # this is affected by previously existing files (but that is not the case of interest for measurement)

    elapsed_time_readable = round(elapsed_time, 2)

    messages_to_log.append('')
    messages_to_log.append(f"The execution of copy_s3_batch took {elapsed_time_readable} seconds, avg {avg_time_per_file} per file\n")

    for m in messages_to_log:
        logger.debug(m)

    # we return the list of sucessfully copied files to be used as an input for the update of db rows
    return sucessfully_copied


# this function updates a batch of database rows
def update_db_batch(db_connection, rows_to_update, dry_run=False):

    # because of process concurrency we need to delay the logs of this function
    # and log them all at once
    messages_to_log = []

    start_time = time.time()

    if dry_run:
        msg_prefix = 'DRY RUN '
    else:
        msg_prefix = ''

    messages_to_log.append('Got DB batch')

    cur = db_connection.cursor()
    nr_updated_rows = 0
    try:
        for row in rows_to_update:
            row_id  = row[0]
            old_key = row[1]
            filename = os.path.basename(old_key)
            new_key = f"avatar/{filename}"

            messages_to_log.append(f"  * {msg_prefix}updating {old_key} to {new_key}")

            if not dry_run:
                cur.execute("UPDATE avatars SET path = %s WHERE id = %s", (new_key, row_id))
            nr_updated_rows += 1
    except Exception as e:
        messages_to_log.append(f"Error updating row {entry}: {e}")

    db_connection.commit()

    messages_to_log.append('DB batch done')

    end_time = time.time()

    elapsed_time = end_time - start_time

    if len(rows_to_update) > 0:
        avg_time_per_row = round(elapsed_time / len(rows_to_update), 2)  # same criterium as for measuring files, affected by row update errors (not frequent, not the case of intereset)
    else:
        avg_time_per_row = -1

    elapsed_time_readable = round(elapsed_time, 2)

    messages_to_log.append('')
    messages_to_log.append(f"The execution of update_db_batch took {elapsed_time_readable} seconds, avg {avg_time_per_row} seconds per row\n")

    for m in messages_to_log:
        logger.debug(m)

    return nr_updated_rows


# this function processes a batch of data in terms of s3 copies and db row updates
def process_batch(bucket_src, bucket_dst, batch, dry_run, overwrite, queue=None):

    db_connection = get_db_connection()
    s3_connection = get_s3_connection()

    # we only update the entries that correspond to files that have been copied
    # files that were already on the destination bucket of files for which there was an error
    # do not have their corresponding db entry updated

    # perform s3 copy
    rows_to_update = copy_s3_batch(s3_connection, bucket_src, bucket_dst, batch, dry_run, overwrite)
    copied_files = len(rows_to_update)

    # update database rows
    updated_rows = update_db_batch(db_connection, rows_to_update, dry_run)

    # we pass the result as dictionary if a queue has been passed as an argument
    # otherwise we use the tradicional return values
    if queue is not None:
        result = { "copied_files": copied_files, "updated_rows": updated_rows }
        queue.put(result)
        return
    else:
        return copied_files, updated_rows


# this function performs the data migration work from a high level perspective
def migrate_legacy_data(db_connection, s3_connection, bucket_src, bucket_dst, start_time, batch_size, dry_run=False, overwrite=False, parallelization_level=1):

    total_copied_files = 0
    total_updated_rows = 0

    if dry_run:
        msg_prefix = 'DRY RUN '
    else:
        msg_prefix = ''

    try:
        cur = db_connection.cursor()

        cur.execute('SELECT COUNT(*) from avatars WHERE path LIKE(\'image/%\');')
        row_count = cur.fetchone()[0]

        nr_batches_to_process = math.ceil(row_count / batch_size)

        start_time = time.time()
        # this SELECT statement fetches the rows that match the legacy pattern
        cur.execute('SELECT * from avatars WHERE path LIKE(\'image/%\');')
        end_time = time.time()

        elapsed_time = round(end_time - start_time, 2)

        logger.debug('')
        logger.debug(f"The execution of SELECT took {elapsed_time} seconds\n")

        # we retreive the entries in batches
        batch = cur.fetchmany(batch_size)

        # Create a queue to pass data between processes
        manager = Manager()
        queue   = manager.Queue()

        nr_batches_processed = 0
        while len(batch) > 0:
            nr_processes = 0
            processes = []
            while nr_processes < parallelization_level and len(batch) > 0:

                args = (bucket_src, bucket_dst, batch, dry_run, overwrite, queue)
                proc = Process(target=process_batch, args=args)
                proc.daemon = True

                processes.append(proc)
                nr_processes += 1

                batch = cur.fetchmany(batch_size)

            # we exited the inner loop because we either reached the desired number of processes
            # or because there are no more batches

            # now let's start the processes
            for p in processes:
                p.start()

            # and wait for their completion
            for p in processes:
                p.join()
                nr_batches_processed += 1

            # and now the lets the results from the queue
            result_list = []
            try:
                # get all the items until the queue is empty
                while True:
                    entry = queue.get(False)
                    result_list.append(entry)

            except Exception as ex:
                pass

            # and proceed with the sums for this process group
            copied_files = 0
            updated_rows = 0
            for result in result_list:
                copied_files += result['copied_files']
                updated_rows += result['updated_rows']

            # and here we calculate the totals
            total_copied_files += copied_files
            total_updated_rows += updated_rows

            # and provide some progress information
            progress_pct = round(nr_batches_processed / nr_batches_to_process * 100)
            cur_time = time.time()
            elapsed_time = round(cur_time - start_time, 2)

            progress_str = f"{msg_prefix}  * Progress {progress_pct:3d}%, batches processed {nr_batches_processed}/{nr_batches_to_process}, files copied {total_copied_files}, rows updated {total_updated_rows}, elapsed time {elapsed_time}"

            logger.info(progress_str)

        cur.close()

    except Exception as e:
        logger.error(f"Error getting file list entries: {e}")
        db_connection.close()
        exit(E_ERR)

    if total_updated_rows != total_copied_files:
        logger.error("ERROR: the number of updated rows should be equal to the number of copied files")
