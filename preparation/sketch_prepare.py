#!/usr/bin/env python

import sys
import os
import logging
import argparse
import psycopg2
import boto3
import botocore
import random

# yes, I know,  but we are importing "constants" from a custom module
# all constants are in use and are UPPER_CASE, no danger in sight
from config import *


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


# generates the avatar path
def generate_path(n):
    prefixes = [ 'image', 'avatar']
    num = 0
    while num < n:
        index = round(random.random())
        prefix = prefixes[index]
        yield f"{prefix}/avatar-{num:09d}.png"
        num += 1


# re-creates the database - previously stored data IS LOST
def create_db(connection):
    try:
        connection.autocommit = True
        cur = connection.cursor()
        cur.execute("DROP DATABASE proddatabase;")
        cur.execute("CREATE DATABASE proddatabase;")
    except Exception as e:
        logging.error(f"Error creating the database: {e}")
        sys.exit(1)


# creates the database table
def init_db(connection):
    try:
        cur = connection.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS avatars ( id SERIAL PRIMARY KEY, path VARCHAR );")
        connection.commit()
    except Exception as e:
        logging.error(f"Error initializing the database: {e}")
        sys.exit(1)


# inserts the reference to an avatar in a table row
def insert_db_row(connection, path):
    try:
        cur = connection.cursor()
        cur.execute("INSERT INTO avatars (path) VALUES (%s)", (path,))
        connection.commit()
    except Exception as e:
        logging.error(f"Error inserting to the database: {e}")
        sys.exit(1)


# deletes every object inside the bucket
def init_bucket(s3_conn, bucket_name, verbose=False):

    response = s3_conn.list_objects_v2(Bucket=bucket_name, MaxKeys=S3_MAX_OBJECTS_REQ)

    try:
        objects = response['Contents']
    except Exception as e:
        if verbose:
            print(f"  * bucket {bucket_name} was already empty")
        return

    # we need to loop because the list_objects_v2 functions never returns more than 1000
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html#list-objects-v2

    nr_deleted_objects_total   = 0

    nr_found_objects_partial = len(objects)
    nr_found_objects_total   = len(objects)

    while nr_found_objects_partial > 0:
        nr_deleted_objects_partial = 0
        for obj in objects:
            key = obj['Key']
            if verbose:
                print('  * deleting', key)
            s3_conn.delete_object(Bucket=bucket_name, Key=key)
            nr_deleted_objects_partial += 1

        print(f"  * partial count: found {nr_found_objects_partial} objects, deleted {nr_deleted_objects_partial} objects")
        nr_deleted_objects_total += nr_deleted_objects_partial

        if not response.get('IsTruncated'):
            break
        else:
            continuation_token = response.get('NextContinuationToken')

        response = s3_conn.list_objects_v2(Bucket=bucket_name, MaxKeys=S3_MAX_OBJECTS_REQ, ContinuationToken=continuation_token)
        try:
            objects = response['Contents']
            nr_found_objects_partial = len(objects)
            nr_found_objects_total += nr_found_objects_partial
            if verbose:
                print(f"  * bucket {bucket_name} has more objects")
        except Exception as e:
            if verbose:
                print(f"  * bucket {bucket_name} has been emptied")
            nr_found_objects_partial = 0

    print(f"  * final count: found {nr_found_objects_total} objects, deleted {nr_deleted_objects_total} objects")


# creates the avatar file in the S3 bucket
def create_s3_object(s3_conn, bucket, path):
    try:
        s3_conn.put_object(Bucket=bucket, Key=f"{path}", Body=DUMMY_AVATAR)
    except Exception as e:
        logging.error(f"Error while creating an s3 object: {e}")
        sys.exit(1)


# checks if the user really wants to move forward
def check_willingness(clean_production_bucket):

    if clean_production_bucket:
        extra_str = f"and bucket {S3_BUCKET_NAME}"
    else:
        extra_str = ""

    warning_str = f"WARNING: this script will DROP and re-CREATE database {DB_NAME} and delete all contents of bucket {S3_BUCKET_NAME_LEG} {extra_str}"

    print(warning_str)

    user_response = input('Are you sure you want to continue? (yes/no) ')

    if user_response != 'yes':
        print('Execution canceled')
        exit(1)
    else:
        print('Execution starting')


# main script
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='This script seeds the database and s3 bucket with the number of legacy avatars passed as a first argument. Previously stored data is deleted.')
    parser.add_argument('number_of_avatars', type=int, help='Number of legacy avatars to create')

    parser.add_argument('-c', '--clean-production', help='Clean also the production bucket', default=False, action='store_true')
    parser.add_argument('-v', '--verbose',          help='Print extra messages',             default=False, action='store_true')
    parser.add_argument('-y', '--say-yes',          help='skip confirmation prompts',        default=False, action='store_true')

    args = parser.parse_args()

    # Check if we have the necessary environment variables defined and fail early otherwise
    check_environment()

    # Check if the user really wants to do this
    if not args.say_yes:
        check_willingness(args.clean_production)

    # Connect to the database server using the default database
    try:
        conn = psycopg2.connect(DB_CONN_STRING_0)
    except Exception as e:
        logging.error(f"Error while connecting to the database server: {e}")
        conn.close()
        sys.exit(1)

    # Create our database
    try:
        print('Re-creating the database')
        create_db(conn)
    except Exception as e:
        logging.error(f"Error while creating the database: {e}")
        conn.close()
        sys.exit(1)

    # Connect to the database server using our database
    try:
        conn = psycopg2.connect(DB_CONN_STRING_1)
    except Exception as e:
        logging.error(f"Error while connecting to the database server using our database: {e}")
        conn.close()
        sys.exit(1)

    # Initialize our database
    try:
        print('Initializing the database')
        init_db(conn)
    except Exception as e:
        logging.error(f"Error while initializing creating the database: {e}")
        conn.close()
        sys.exit(1)

    # Initialize s3 connection
    try:
        # we are using the traditional cient API instead of the newer resource API because
        # the enumeration of objects using the resource API was not working on Digital Ocean
        # the traditional API seems to be the recommended practice at this point
        #
        # https://docs.digitalocean.com/products/spaces/reference/s3-sdk-examples/
        #
        # and it probably maximizes compatibility with other vendors as well
        #
        # see also:
        # https://stackoverflow.com/questions/65687417/list-all-objects-in-digitalocean-bucket

        # furthermore, enumerating objects might turn out to be too slow in a real world case
        # but deleting + recreating a bucket also opens the window for race conditions related
        # to a bucket name being or note available directly after deletion (I've seen this on AWS...)

        # bottom line: this is good enough for a demo, production use would require further examination

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

    # Clean the bucket
    try:
        print('Cleaning the legacy bucket')
        init_bucket(s3, S3_BUCKET_NAME_LEG, args.verbose)

        if args.clean_production:
            print('Cleaning the production bucket')
            init_bucket(s3, S3_BUCKET_NAME, args.verbose)

    except Exception as e:
        logging.error(f"Error while cleaning the S3 bucket: {e}")
        sys.exit(1)

    # Generate as many avatars as requested
    print('Creating S3 objects and the corresponding database rows')

    legacy_avatars = 0
    production_avatars = 0

    for path in generate_path(args.number_of_avatars):
        if args.verbose:
            print('  * creating', path)

        # all the generated paths are added to the database but only
        # the legacy ones (image/) are added to the legacy bucket

        insert_db_row(conn, path)

        if 'image/' in path:
            if args.verbose:
                print('    - added to the legacy bucket')
            create_s3_object(s3, S3_BUCKET_NAME_LEG, path)
            legacy_avatars += 1
        else:
            if args.verbose:
                print('    - added to the production bucket')
            create_s3_object(s3, S3_BUCKET_NAME, path)
            production_avatars += 1

    conn.close()

    print(f"\nCreated {legacy_avatars} legacy avatars and {production_avatars} production avatars")
