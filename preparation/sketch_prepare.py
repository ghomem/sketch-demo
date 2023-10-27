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
    num = 0
    while num < n:
        yield f"image/avatar-{num:03d}.png"
        num += 1


# re-creates the database - previously stored data IS LOST
def create_db(connection):
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("DROP DATABASE proddatabase;")
        cur.execute("CREATE DATABASE proddatabase;")
    except Exception as e:
        logging.error(f"Error creating the database: {e}")
        sys.exit(1)


# creates the database table
def init_db(connection):
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS avatars ( id SERIAL PRIMARY KEY, path VARCHAR );")
        conn.commit()
    except Exception as e:
        logging.error(f"Error initializing the database: {e}")
        sys.exit(1)


# inserts the reference to an avatar in a table row
def insert_db_row(connection, path):
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO avatars (path) VALUES (%s)", (path,))
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting to the database: {e}")
        sys.exit(1)


# deletes every object inside the bucket
def init_bucket(bucket_name, verbose=False):

    # we are using the traditional cient API instead of the newer resource API because 
    # the enumeration of objects using the resource API was not working on Digital Ocean
    # the traditional API seems to be the recommended practice at this point
    # https://docs.digitalocean.com/products/spaces/reference/s3-sdk-examples/
    #
    # see also: 
    # https://stackoverflow.com/questions/65687417/list-all-objects-in-digitalocean-bucket

    # furthermore, enumerating objects might turn out to be too slow in a real world case
    # but deleting + recreating a bucket also opens the window for race conditions related
    # to a bucket name being or note available directly after deletion (I've seen this on AWS...)

    # bottom line: this is good enough for a demo, production use would require further examination

    session = boto3.session.Session()
    client = session.client('s3',
                            config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
                            region_name=AWS_DEFAULT_REGION,
                            endpoint_url=S3_ENDPOINT_URL_LEG,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    response = client.list_objects(Bucket=bucket_name)
    for obj in response['Contents']:
        key = obj['Key']
        if verbose:
            print('  * deleting', key)
        client.delete_object(Bucket=bucket_name, Key=key)


# creates the avatar file in the S3 bucket
def create_s3_object(s3_conn, bucket, path):
    try:
        if AVATAR_FILE:
            s3_conn.Bucket(bucket).upload_file(Key=f"{path}", Filename=AVATAR_FILE)
        else:
            s3_conn.Bucket(bucket).put_object(Key=f"{path}", Body=DUMMY_AVATAR)
    except Exception as e:
        logging.error(f"Error while creating an s3 object: {e}")
        sys.exit(1)


# main script
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='This script seeds the database and s3 bucket with the number of legacy avatars passed as a first argument. Previsouly stored data is deleted.')
    parser.add_argument('number_of_avatars', type=int, help='Number of legacy avatars to create')
    parser.add_argument( '-v', '--verbose', help='print extra messages', default=False, action='store_true')
    args = parser.parse_args()

    # Check if we have the necessary environment variables defined and fail early otherwise
    check_environment()

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
        s3 = boto3.resource('s3',
                            endpoint_url=S3_ENDPOINT_URL,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=AWS_DEFAULT_REGION
                            )
    except Exception as e:
        logging.error(f"Error while connecting to S3: {e}")
        sys.exit(1)

    # Clean the bucket
    try:
        print('Cleaning the legacy bucket')
        init_bucket(S3_BUCKET_NAME, args.verbose)
    except Exception as e:
        logging.error(f"Error while cleaning the S3 bucket: {e}")
        sys.exit(1)

    # Generate as many legacy avatars as requested
    print('Creating S3 objects and the corresponding database rows')
    for path in generate_path(args.number_of_avatars):
        if args.verbose:
            print('  * creating', path)
        insert_db_row(conn, path)
        create_s3_object(s3, S3_BUCKET_NAME, path)

    conn.close()

    print(f"Created {args.number_of_avatars} legacy avatars")
