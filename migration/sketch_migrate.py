#!/usr/bin/env python

import sys
import os
import time
import logging
import argparse
import psycopg2
import boto3
import botocore

# yes, I know,  but we are importing "constants" from a custom module
# all constants are in use and are UPPER_CASE, no danger in sight
from config import *


# FIXME it is not ADMIN user in the final version MIGRATION_USER
# checks if the mandatory environment variables are defined
def check_environment():

    if DB_USER is None:
        logging.error('the SKETCH_DB_ADMIN_USER environment variable is not defined')
        exit(1)

    if DB_PASS is None:
        logging.error('the SKETCH_DB_ADMIN_PASS environment variable is not defined')
        exit(1)

    if AWS_ACCESS_KEY_ID is None:
        logging.error('the AWS_ACCESS_KEY_ID environment variable is not defined')
        exit(1)

    if AWS_SECRET_ACCESS_KEY is None:
        logging.error('the AWS_SECRET_ACCESS_KEY is not defined')
        exit(1)


# this function copies a batch of legacy files present on the legacy bucket to the production bucket
def copy_s3_batch(s3_connection, bucket_src, bucket_dst, batch, verbose=False):

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
                print(f"  * copying {bucket_src}/{old_key} to {bucket_dst}/{new_key}")

        if skip is not True:
            try:
                # reference https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/copy.html#copy
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
def update_db_batch(connection, rows_to_update, verbose=False):

    if verbose:
        print("Got DB batch")

    cur = connection.cursor()
    nr_updated_rows = 0
    try:
        for row in rows_to_update:
            row_id  = row[0]
            old_key = row[1]
            filename = os.path.basename(old_key)
            new_key = f"avatar/{filename}"

            if verbose:
                print(f"  * updating {old_key} to {new_key}")

            cur.execute("UPDATE avatars SET path = %s WHERE id = %s", (new_key, row_id))
            nr_updated_rows += 1
    except Exception as e:
        logging.error(f"Error updating row {entry}: {e}")

    connection.commit()

    if verbose:
        print("DB batch done")

    return nr_updated_rows


# high level function to perform the data migration work
def migrate_legacy_data(connection, s3_connection, bucket_src, bucket_dst, batch_size, verbose=False):

    total_copied_files = 0
    total_updated_rows = 0

    try:
        cur = connection.cursor()

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

            # we only update the entries that correspond to files that have been copied
            # files that were already on the destination bucket of files for which there was an error
            # do not have their corresponding db entry updated

            # perform s3 copy
            start_time = time.time()
            rows_to_update = copy_s3_batch(s3_connection, bucket_src, bucket_dst, batch, verbose)
            end_time = time.time()

            elapsed_time = end_time - start_time
            avg_time_per_file = round(elapsed_time / len(batch), 2)  # this is affected by previously existing files (but that is not the case of interest for measurement)

            elapsed_time_readable = round(elapsed_time, 2)

            if verbose:
                print(f"\nThe execution of copy_s3_batch took {elapsed_time_readable} seconds, avg {avg_time_per_file} per file\n")

            # update database rows
            start_time = time.time()
            updated_rows = update_db_batch(connection, rows_to_update, verbose)
            end_time = time.time()

            elapsed_time = end_time - start_time
            if len(rows_to_update) > 0:
                avg_time_per_row = round(elapsed_time / len(rows_to_update), 2)  # same criterium as for measuring files, affected by row update errors (not frequent, not the case of intereset)
            else:
                avg_time_per_row = -1

            elapsed_time_readable = round(elapsed_time, 2)

            if verbose:
                print(f"\nThe execution of update_db_batch took {elapsed_time_readable} seconds, avg {avg_time_per_row} seconds per row\n")

            total_copied_files += len(rows_to_update)
            total_updated_rows += updated_rows

            batch = cur.fetchmany(batch_size)

        cur.close()

    except Exception as e:
        logging.error(f"Error getting file list entries: {e}")
        connection.close()
        sys.exit(1)

    print(f"Copied {total_copied_files} files")
    print(f"Updated {total_updated_rows} rows")

    if total_updated_rows != total_copied_files:
        print("ERROR: the number of updated rows should be equal to the number of copied files")


# checks if the user really wants to move forward
def check_willingness():

    print(f"WARNING: this script will modify rows at database {DB_NAME} after copying the files at {S3_BUCKET_NAME_LEG} to {S3_BUCKET_NAME}.")

    user_response = input('Are you sure you want to continue? (yes/no) ')

    if user_response != 'yes':
        print('Execution canceled')
        exit(1)
    else:
        print('Execution starting')


# main script
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='This script migrate files from the legacy bucket to the current production bucket and edits the corresponding database entries.')

    parser.add_argument('batch_size', type=int, help='Number of copies to process at once')
    parser.add_argument( '-v', '--verbose', help='print extra messages', default=False, action='store_true')

    args = parser.parse_args()

    # Check if we have the necessary environment variables defined and fail early otherwise
    check_environment()

    # Check if the user really wants to do this
    check_willingness()

    print('Connecting to the database')

    # Connect to the database server using our database
    try:
        conn = psycopg2.connect(DB_CONN_STRING)
    except Exception as e:
        logging.error(f"Error while connecting to the database server using our database: {e}")
        conn.close()
        sys.exit(1)

    print('Connecting to the S3 storage')

    # Initialize s3 connection
    try:
        session = boto3.session.Session()
        s3 = session.client('s3',
                            config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
                            region_name=AWS_DEFAULT_REGION,
                            endpoint_url=S3_ENDPOINT_URL_LEG,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    except Exception as e:
        logging.error(f"Error while connecting to S3: {e}")
        sys.exit(1)

    print('Migrating legacy data')

    start_time = time.time()

    migrate_legacy_data(conn, s3, S3_BUCKET_NAME_LEG, S3_BUCKET_NAME, args.batch_size, args.verbose)

    end_time = time.time()

    elapsed_time = round(end_time - start_time, 2)

    print(f"Execution finished after {elapsed_time} seconds")
