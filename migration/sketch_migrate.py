#!/usr/bin/env python

import sys
import os
import time
import logging
import argparse


# yes, I know,  but we are importing "constants" from a custom module
# all constants are in use and are UPPER_CASE, no danger in sight
from lib.config import *

from lib.libmig import ( copy_s3_batch, update_db_batch, migrate_legacy_data, get_db_connection, get_s3_connection, get_log_filename,
                         check_status, check_bucket_read_permissions, check_bucket_write_permissions )


# we obtain the logger declared in main for use within this module
logger = logging.getLogger("miglogger")


# checks if the mandatory environment variables are defined
def check_environment():

    if DB_USER is None:
        logger.error('the SKETCH_DB_USER environment variable is not defined')
        exit(E_ERR)

    if DB_PASS is None:
        logger.error('the SKETCH_DB_PASS environment variable is not defined')
        exit(E_ERR)

    if AWS_ACCESS_KEY_ID is None:
        logger.error('the AWS_ACCESS_KEY_ID environment variable is not defined')
        exit(E_ERR)

    if AWS_SECRET_ACCESS_KEY is None:
        logger.error('the AWS_SECRET_ACCESS_KEY is not defined')
        exit(E_ERR)


# checks if the user really wants to move forward
def check_willingness(overwrite):

    logger.info(f"WARNING: this script will modify rows at database {DB_NAME} after copying the files at {S3_BUCKET_NAME_LEG} to {S3_BUCKET_NAME}.")

    if overwrite:
        logger.info(f"WARNING: this script will overwrite files at {S3_BUCKET_NAME} if files with the same name exist at {S3_BUCKET_NAME_LEG}.")

    user_response = input('Are you sure you want to continue? (yes/no) ')

    logger.info('')
    if user_response != 'yes':
        logger.info('Execution canceled')
        exit(E_ERR)
    else:
        logger.info('Execution starting')


def main():

    parser = argparse.ArgumentParser(description='This script migrates files from the legacy bucket to the current production bucket and updates the corresponding database entries.')

    # optional parameters
    parser.add_argument('-p', '--parallelization-level', help='number of parallel worker processes',        type=int, default=1)
    parser.add_argument('-b', '--batch-size',            help='number of db and s3 entries per iteration',  type=int, default=20)
    parser.add_argument('-l', '--limit',                 help='limit for the number of entries to migrate', type=int, default=0)

    # flags
    parser.add_argument('-v', '--verbose',          help='print extra messages',                            default=False, action='store_true')
    parser.add_argument('-d', '--dry-run',          help='simulate execution without actually executing',   default=False, action='store_true')
    parser.add_argument('-w', '--overwrite',        help='allow overwriting of existing files',             default=False, action='store_true')
    parser.add_argument('-s', '--status-only',      help='only print the data status',                      default=False, action='store_true')
    parser.add_argument('-t', '--technical-status', help='print a line with the numbers at the end',        default=False, action='store_true')
    parser.add_argument('-y', '--say-yes',          help='skip confirmation prompts',                       default=False, action='store_true')

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

    # basic sanity check on the inputs
    if args.batch_size < 1 or args.parallelization_level < 1:
        logger.error('batch size and parallelization level must be positive integers')
        exit(E_ERR)

    if args.limit < 0:
        logger.error('limit must be greater than or equal to zero')
        exit(E_ERR)

    # Check if we have the necessary environment variables defined and fail early otherwise
    check_environment()

    # Check if the user really wants to migrate
    # unless are only printing the status or the user disables confirmations prompts
    if not args.status_only and not args.say_yes:
        check_willingness(args.overwrite)

    logger.info('Connecting to the database')

    # Connect to the database server using our database
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error(f"Error while connecting to the database server {DB_HOST} with database {DB_NAME} and user {DB_USER}")
        logger.error('  * please check the database hostname and credentials.')
        exit(E_ERR)

    logger.info('Connecting to the S3 storage')

    # Initialize s3 connection
    try:
        s3_conn = get_s3_connection()
    except Exception as e:
        logger.error(f"Error while connecting to S3: {e}")
        exit(E_ERR)

    # Check if we have the necessary permissions on each buckets

    logger.info(f"Checking S3 read permissions for {S3_BUCKET_NAME_LEG} on {S3_BUCKET_DOMAIN}")

    check_bucket_read_permissions(s3_conn, S3_BUCKET_NAME_LEG)

    logger.info(f"Checking S3 write permissions for {S3_BUCKET_NAME} on {S3_BUCKET_DOMAIN}")

    check_bucket_write_permissions(s3_conn, S3_BUCKET_NAME)

    # Check the status and reconfirm that the user wants to migrate from this status, if necessary
    if args.status_only:
        status = check_status(conn, s3_conn, False)
        if args.technical_status:
            print(f"\ntech_status {status}")
        conn.close()
        exit(E_OK)
    else:
        check_status(conn, s3_conn, not args.say_yes)

    logger.info('Migrating legacy data')

    logger.info(f"The log file for this execution will be {log_file}")

    start_time = time.time()

    logger.info('')
    logger.info('Progress information:')
    migrate_legacy_data(conn, s3_conn, S3_BUCKET_NAME_LEG, S3_BUCKET_NAME, start_time, args.batch_size, args.limit, args.dry_run, args.overwrite, args.parallelization_level)
    logger.info('')

    end_time = time.time()

    elapsed_time = round(end_time - start_time, 2)

    logger.info(f"Execution finished after {elapsed_time} seconds")

    status = check_status(conn, s3_conn, False)

    # extra copy/paste niceness for the user
    print('\nThe log file can be reviewed with:')
    print(f"less {log_file}")

    if args.technical_status:
        print(f"\ntech_status {status}")

    exit(E_OK)


# main script
if __name__ == "__main__":
    main()
