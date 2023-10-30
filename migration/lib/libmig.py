import sys
import os
import time
import logging
import psycopg2
import boto3
import botocore

from lib.config import *


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


# this function copies a batch of legacy files present on the legacy bucket to the production bucket
def copy_s3_batch(s3_connection, bucket_src, bucket_dst, batch, verbose=False, dry_run=False):

    if dry_run:
        msg_prefix = 'DRY RUN '
    else:
        msg_prefix = ''

    if verbose:
        print("Got S3 batch")

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
            if verbose:
                print(f"  * skipping {bucket_src}/{old_key} as {bucket_dst}/{new_key} already exists")
        except Exception as e:
            skip = False
            if verbose:
                print(f"  * {msg_prefix}copying {bucket_src}/{old_key} to {bucket_dst}/{new_key}")

        if skip is not True:
            try:
                # reference https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/copy.html#copy
                if not dry_run:
                    s3_connection.copy(copy_source, bucket_dst, new_key)
                # we store the list of sucessfully copied files
                sucessfully_copied.append(row)
            except Exception as e:
                logging.error(f"Error copying file {old_key}: {e}")

    if verbose:
        print("S3 batch done")

    # we return the list of sucessfully copied files to be used as an input for the update of db rows
    return sucessfully_copied


# this function updates a batch of database rows
def update_db_batch(db_connection, rows_to_update, verbose=False, dry_run=False):

    if dry_run:
        msg_prefix = 'DRY RUN '
    else:
        msg_prefix = ''

    if verbose:
        print("Got DB batch")

    cur = db_connection.cursor()
    nr_updated_rows = 0
    try:
        for row in rows_to_update:
            row_id  = row[0]
            old_key = row[1]
            filename = os.path.basename(old_key)
            new_key = f"avatar/{filename}"

            if verbose:
                print(f"  * {msg_prefix}updating {old_key} to {new_key}")

            if not dry_run:
                cur.execute("UPDATE avatars SET path = %s WHERE id = %s", (new_key, row_id))
            nr_updated_rows += 1
    except Exception as e:
        logging.error(f"Error updating row {entry}: {e}")

    db_connection.commit()

    if verbose:
        print("DB batch done")

    return nr_updated_rows


# this function processes a batch of data in terms of s3 copies and db row updates
def process_batch(db_connection, s3_connection, bucket_src, bucket_dst, batch, verbose, dry_run):

    # we only update the entries that correspond to files that have been copied
    # files that were already on the destination bucket of files for which there was an error
    # do not have their corresponding db entry updated

    # perform s3 copy
    start_time = time.time()
    rows_to_update = copy_s3_batch(s3_connection, bucket_src, bucket_dst, batch, verbose, dry_run)
    end_time = time.time()

    elapsed_time = end_time - start_time
    avg_time_per_file = round(elapsed_time / len(batch), 2)  # this is affected by previously existing files (but that is not the case of interest for measurement)

    elapsed_time_readable = round(elapsed_time, 2)

    if verbose:
        print(f"\nThe execution of copy_s3_batch took {elapsed_time_readable} seconds, avg {avg_time_per_file} per file\n")

    # update database rows
    start_time = time.time()
    updated_rows = update_db_batch(db_connection, rows_to_update, verbose, dry_run)
    end_time = time.time()

    elapsed_time = end_time - start_time
    if len(rows_to_update) > 0:
        avg_time_per_row = round(elapsed_time / len(rows_to_update), 2)  # same criterium as for measuring files, affected by row update errors (not frequent, not the case of intereset)
    else:
        avg_time_per_row = -1

    elapsed_time_readable = round(elapsed_time, 2)

    if verbose:
        print(f"\nThe execution of update_db_batch took {elapsed_time_readable} seconds, avg {avg_time_per_row} seconds per row\n")

    copied_files = len(rows_to_update)

    return copied_files, updated_rows


# this function performs the data migration work from a high level perspective
def migrate_legacy_data(db_connection, s3_connection, bucket_src, bucket_dst, batch_size, verbose=False, dry_run=False):

    total_copied_files = 0
    total_updated_rows = 0

    try:
        cur = db_connection.cursor()

        start_time = time.time()
        # this SELECT statement fetches the rows that match the legacy pattern
        cur.execute('SELECT * from avatars WHERE path LIKE(\'image/%\');')
        end_time = time.time()

        elapsed_time = round(end_time - start_time, 2)

        if verbose:
            print(f"\nThe execution of SELECT took {elapsed_time} seconds\n")

        # we retreive the entries in batches
        batch = cur.fetchmany(batch_size)

        while len(batch) > 0:

            copied_files, updated_rows = process_batch(db_connection, s3_connection, bucket_src, bucket_dst, batch, verbose, dry_run)
            batch = cur.fetchmany(batch_size)

            total_copied_files += copied_files
            total_updated_rows += updated_rows

        cur.close()

    except Exception as e:
        logging.error(f"Error getting file list entries: {e}")
        db_connection.close()
        sys.exit(1)

    if dry_run:
        msg_prefix = 'DRY RUN '
    else:
        msg_prefix = ''

    print(f"{msg_prefix}Copied {total_copied_files} files")
    print(f"{msg_prefix}Updated {total_updated_rows} rows")

    if total_updated_rows != total_copied_files:
        print("ERROR: the number of updated rows should be equal to the number of copied files")
