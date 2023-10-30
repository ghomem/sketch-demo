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
from lib.config import *

from lib.libmig import copy_s3_batch, update_db_batch, migrate_legacy_data, get_db_connection, get_s3_connection


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

    parser = argparse.ArgumentParser(description='This script migrates files from the legacy bucket to the current production bucket and updates the corresponding database entries.')

    # positional required argument
    parser.add_argument('batch_size',       help='number of copies to process at once', type=int)

    # flags
    parser.add_argument( '-v', '--verbose', help='print extra messages',                          default=False, action='store_true')
    parser.add_argument( '-d', '--dry-run', help='simulate execution without actually executing', default=False, action='store_true')

    args = parser.parse_args()

    # Check if we have the necessary environment variables defined and fail early otherwise
    check_environment()

    # Check if the user really wants to do this
    check_willingness()

    print('Connecting to the database')

    # Connect to the database server using our database
    try:
        conn = get_db_connection()
    except Exception as e:
        logging.error(f"Error while connecting to the database server using our database: {e}")
        conn.close()
        sys.exit(1)

    print('Connecting to the S3 storage')

    # Initialize s3 connection
    try:
        s3_conn = get_s3_connection()
    except Exception as e:
        logging.error(f"Error while connecting to S3: {e}")
        sys.exit(1)

    print('Migrating legacy data')

    start_time = time.time()

    migrate_legacy_data(conn, s3_conn, S3_BUCKET_NAME_LEG, S3_BUCKET_NAME, args.batch_size, args.verbose, args.dry_run)

    end_time = time.time()

    elapsed_time = round(end_time - start_time, 2)

    print(f"Execution finished after {elapsed_time} seconds")
