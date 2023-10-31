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

    random_str_1 = get_random_string(24, CHARSET_TMP)
    random_str_2 = get_random_string(24, CHARSET_TMP)

    # this file certainly does not exist :-)
    # the last component is just for the unlikely case that there is a problem deleting yet
    # so that we know where it came from
    return f"{random_str_1}/{random_str_2}.sketch_migration_gustavo"


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


# this function summarizes the S3 status
def check_status(db_connection, s3_connection, request_confirmation):

    logger.info('')
    logger.info('Current data status:')
    nr_found_objects_legacy     = check_s3_status(s3_connection, S3_BUCKET_NAME_LEG)
    nr_found_objects_production = check_s3_status(s3_connection, S3_BUCKET_NAME)
    nr_found_objects_total = nr_found_objects_legacy + nr_found_objects_production

    logger.info(f"  * {nr_found_objects_total} total objects found\n")

    check_db_status(db_connection)

    logger.info('')
    logger.info('NOTE: this script does not delete legacy objects')

    if request_confirmation:
        user_response = input('\nAre you sure you want perform the migration over this data status? (yes/no) ')

        if user_response != 'yes':
            logger.info('')
            logger.info('Execution canceled')
            exit(1)
        else:
            logger.info('')
            return


# this function checks if we have write permissions on the production bucket
def check_bucket_write_permissions(s3_connection, bucket_name):

    try:
        hello_world_key = get_random_keyname()
        s3_connection.put_object(Bucket=bucket_name, Key=hello_world_key, Body='hello world!')
        s3_connection.delete_object(Bucket=bucket_name, Key=hello_world_key)
    except Exception as e:
        logger.error(f"Error while creating an test s3 object: {e}")
        logger.error('Check your bucket write permissions')
        sys.exit(1)


# this function copies a batch of legacy files present on the legacy bucket to the production bucket
def copy_s3_batch(s3_connection, bucket_src, bucket_dst, batch, dry_run=False):

    if dry_run:
        msg_prefix = 'DRY RUN '
    else:
        msg_prefix = ''

    logger.debug("Got S3 batch")

    sucessfully_copied = []
    for row in batch:
        old_key = row[1]
        copy_source = { 'Bucket': bucket_src, 'Key': old_key }
        filename = os.path.basename(old_key)
        new_key = f"avatar/{filename}"

        # check first if an object with the same key is already in the production bucket
        # for performance, integrity and idempotency reasons we do not overwrite an existing file on bucket_dst
        response = s3_connection.list_objects_v2(Bucket=bucket_dst, Prefix=new_key)

        try:
            objects = response['Contents']
            skip = True
            logger.debug(f"  * skipping {bucket_src}/{old_key} as {bucket_dst}/{new_key} already exists")
        except Exception as e:
            skip = False
            logger.debug(f"  * {msg_prefix}copying {bucket_src}/{old_key} to {bucket_dst}/{new_key}")

        if skip is not True:
            try:
                # reference https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/copy.html#copy
                if not dry_run:
                    s3_connection.copy(copy_source, bucket_dst, new_key)
                # we store the list of sucessfully copied files
                sucessfully_copied.append(row)
            except Exception as e:
                logger.error(f"Error copying file {old_key}: {e}")

    logger.debug("S3 batch done")

    # we return the list of sucessfully copied files to be used as an input for the update of db rows
    return sucessfully_copied


# this function updates a batch of database rows
def update_db_batch(db_connection, rows_to_update, dry_run=False):

    if dry_run:
        msg_prefix = 'DRY RUN '
    else:
        msg_prefix = ''

    logger.debug("Got DB batch")

    cur = db_connection.cursor()
    nr_updated_rows = 0
    try:
        for row in rows_to_update:
            row_id  = row[0]
            old_key = row[1]
            filename = os.path.basename(old_key)
            new_key = f"avatar/{filename}"

            logger.debug(f"  * {msg_prefix}updating {old_key} to {new_key}")

            if not dry_run:
                cur.execute("UPDATE avatars SET path = %s WHERE id = %s", (new_key, row_id))
            nr_updated_rows += 1
    except Exception as e:
        logger.error(f"Error updating row {entry}: {e}")

    db_connection.commit()

    logger.debug("DB batch done")

    return nr_updated_rows


# this function processes a batch of data in terms of s3 copies and db row updates
def process_batch(db_connection, s3_connection, bucket_src, bucket_dst, batch, dry_run):

    # we only update the entries that correspond to files that have been copied
    # files that were already on the destination bucket of files for which there was an error
    # do not have their corresponding db entry updated

    # perform s3 copy
    start_time = time.time()
    rows_to_update = copy_s3_batch(s3_connection, bucket_src, bucket_dst, batch, dry_run)
    end_time = time.time()

    elapsed_time = end_time - start_time
    avg_time_per_file = round(elapsed_time / len(batch), 2)  # this is affected by previously existing files (but that is not the case of interest for measurement)

    elapsed_time_readable = round(elapsed_time, 2)

    logger.debug('')
    logger.debug(f"The execution of copy_s3_batch took {elapsed_time_readable} seconds, avg {avg_time_per_file} per file\n")

    # update database rows
    start_time = time.time()
    updated_rows = update_db_batch(db_connection, rows_to_update, dry_run)
    end_time = time.time()

    elapsed_time = end_time - start_time
    if len(rows_to_update) > 0:
        avg_time_per_row = round(elapsed_time / len(rows_to_update), 2)  # same criterium as for measuring files, affected by row update errors (not frequent, not the case of intereset)
    else:
        avg_time_per_row = -1

    elapsed_time_readable = round(elapsed_time, 2)

    logger.debug('')
    logger.debug(f"The execution of update_db_batch took {elapsed_time_readable} seconds, avg {avg_time_per_row} seconds per row\n")

    copied_files = len(rows_to_update)

    return copied_files, updated_rows


# this function performs the data migration work from a high level perspective
def migrate_legacy_data(db_connection, s3_connection, bucket_src, bucket_dst, batch_size, dry_run=False):

    total_copied_files = 0
    total_updated_rows = 0

    try:
        cur = db_connection.cursor()

        start_time = time.time()
        # this SELECT statement fetches the rows that match the legacy pattern
        cur.execute('SELECT * from avatars WHERE path LIKE(\'image/%\');')
        end_time = time.time()

        elapsed_time = round(end_time - start_time, 2)

        logger.debug('')
        logger.debug(f"The execution of SELECT took {elapsed_time} seconds\n")

        # we retreive the entries in batches
        batch = cur.fetchmany(batch_size)

        while len(batch) > 0:

            copied_files, updated_rows = process_batch(db_connection, s3_connection, bucket_src, bucket_dst, batch, dry_run)
            batch = cur.fetchmany(batch_size)

            total_copied_files += copied_files
            total_updated_rows += updated_rows

        cur.close()

    except Exception as e:
        logger.error(f"Error getting file list entries: {e}")
        db_connection.close()
        sys.exit(1)

    if dry_run:
        msg_prefix = 'DRY RUN '
    else:
        msg_prefix = ''

    logger.info(f"{msg_prefix}Copied {total_copied_files} files")
    logger.info(f"{msg_prefix}Updated {total_updated_rows} rows")

    if total_updated_rows != total_copied_files:
        logger.debug("ERROR: the number of updated rows should be equal to the number of copied files")
