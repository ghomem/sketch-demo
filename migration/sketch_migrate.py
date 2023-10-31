#!/usr/bin/env python

import sys
import os
import time
import logging
import argparse


# yes, I know,  but we are importing "constants" from a custom module
# all constants are in use and are UPPER_CASE, no danger in sight
from lib.config import *

from lib.libmig import copy_s3_batch, update_db_batch, migrate_legacy_data, get_db_connection, get_s3_connection, get_log_filename, check_status


# we obtain the logger declared in main for use within this module
logger = logging.getLogger("miglogger")


# FIXME it is not ADMIN user in the final version MIGRATION_USER
# checks if the mandatory environment variables are defined
def check_environment():

    if DB_USER is None:
        logger.error('the SKETCH_DB_ADMIN_USER environment variable is not defined')
        exit(1)

    if DB_PASS is None:
        logger.error('the SKETCH_DB_ADMIN_PASS environment variable is not defined')
        exit(1)

    if AWS_ACCESS_KEY_ID is None:
        logger.error('the AWS_ACCESS_KEY_ID environment variable is not defined')
        exit(1)

    if AWS_SECRET_ACCESS_KEY is None:
        logger.error('the AWS_SECRET_ACCESS_KEY is not defined')
        exit(1)


# checks if the user really wants to move forward
def check_willingness():

    logger.info(f"WARNING: this script will modify rows at database {DB_NAME} after copying the files at {S3_BUCKET_NAME_LEG} to {S3_BUCKET_NAME}.")

    user_response = input('Are you sure you want to continue? (yes/no) ')

    logger.info('')
    if user_response != 'yes':
        logger.info('Execution canceled')
        exit(1)
    else:
        logger.info('Execution starting')


def main():

    parser = argparse.ArgumentParser(description='This script migrates files from the legacy bucket to the current production bucket and updates the corresponding database entries.')

    # positional required argument
    parser.add_argument('batch_size',       help='number of copies to process at once', type=int)

    # flags
    parser.add_argument( '-v', '--verbose',     help='print extra messages',                          default=False, action='store_true')
    parser.add_argument( '-d', '--dry-run',     help='simulate execution without actually executing', default=False, action='store_true')
    parser.add_argument( '-s', '--status-only', help='only print the data status',                    default=False, action='store_true')

    args = parser.parse_args()

    # logging logistics
    # we just want a logger that adapts to the user selected verbosity
    # without printing messages of the used components such as boto3
    # the name is arbitraty but we use it to request the logger outside main()
    logger = logging.getLogger("miglogger")

    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logger.setLevel(log_level)

    # for the console handler we use stdout instead of the default (stderr) so that greps can be used
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # for the console we want regular formatting, it is easier to read
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    # we also want to log to a file, we use one log file per execution, not an incremental log file
    log_file = get_log_filename()

    # for the file handler we use the custom named log file whose name was obtained above
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(log_level)

    # for the file formatter we want timestaps for traceability from the future
    file_formatter = logging.Formatter('%(asctime)s %(message)s')
    file_handler.setFormatter(file_formatter)

    # now lets add both handlers to our logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Check if we have the necessary environment variables defined and fail early otherwise
    check_environment()

    # Check if the user really wants to do this
    if not args.status_only:
        check_willingness()

    logger.info('Connecting to the database')

    # Connect to the database server using our database
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error(f"Error while connecting to the database server using our database: {e}")
        conn.close()
        sys.exit(1)

    logger.info('Connecting to the S3 storage')

    # Initialize s3 connection
    try:
        s3_conn = get_s3_connection()
    except Exception as e:
        logger.error(f"Error while connecting to S3: {e}")
        sys.exit(1)

    # Check the status and reconfirm that the user wants to migrate from this status, if necessary
    if args.status_only:
        check_status(conn, s3_conn, False)
        conn.close()
        exit(0)
    else:
        check_status(conn, s3_conn, True)

    logger.info('Migrating legacy data')

    logger.info(f"The log file for this execution will be {log_file}")

    start_time = time.time()

    migrate_legacy_data(conn, s3_conn, S3_BUCKET_NAME_LEG, S3_BUCKET_NAME, args.batch_size, args.dry_run)

    end_time = time.time()

    elapsed_time = round(end_time - start_time, 2)

    logger.info(f"Execution finished after {elapsed_time} seconds")

    check_status(conn, s3_conn, False)

    # extra copy/paste niceness for the user
    print('\nThe log file can be reviewed with:')
    print(f"less {log_file}")


# main script
if __name__ == "__main__":
    main()
