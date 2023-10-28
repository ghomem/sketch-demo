#!/usr/bin/env python

import sys
import os
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
        print("got batch")

    for entry in batch:
        old_key = entry[1]
        copy_source = { 'Bucket': bucket_src, 'Key': old_key }
        filename = os.path.basename(old_key)
        new_key = f"avatar/{filename}"

        # check first if an object with the same key is already in the production bucket
        # for performance, integrity and idenpotency reasons we do not overwrite an existing file on bucket_dst
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
            except Exception as e:
                logging.error(f"Error copying file {old_key}: {e}")

    if verbose:
        print("batch done")


# high level function to do the file migration work
def migrate_legacy_files(connection, s3_connection, bucket_src, bucket_dst, batch_size, verbose=False):

    try:
        cur = connection.cursor()

        # this SELECT statement fetches the rows that match the legacy pattern
        cur.execute('SELECT * from avatars WHERE path LIKE(\'image/%\');')

        # we retreive the entries in batches
        batch = cur.fetchmany(batch_size)

        while len(batch) > 0:
            # TODO return the list of problematic files, multiprocessing
            copy_s3_batch(s3_connection, bucket_src, bucket_dst, batch, verbose)
            batch = cur.fetchmany(batch_size)

        cur.close()

    except Exception as e:
        logging.error(f"Error getting file list entries: {e}")
        connection.close()
        sys.exit(1)


# FIXME update_db_row
# inserts the reference to an avatar in a table row
def insert_db_row(connection, path):
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO avatars (path) VALUES (%s)", (path,))
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting to the database: {e}")
        connection.close()
        sys.exit(1)


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

    print('Copying legacy files')

    # first we copy the files to the new bucket, the old ones remain where they were
    # therefore the database entries reman functional (pointing to the old ones)
    migrate_legacy_files(conn, s3, S3_BUCKET_NAME_LEG, S3_BUCKET_NAME, args.batch_size, args.verbose)

    # TODO now we migrate the database entries

    print('Execution finished')
