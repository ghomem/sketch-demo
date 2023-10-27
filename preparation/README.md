## Introduction
This script prepares the environment for a demo migration procedure by populating an S3 bucket with autogenerated files and a PostgreSQL database with the corresponding path entries.

**WARNING** To ensure clean execution conditions, this script deletes previously existing data in the database and bucket. Do not execute if this is not intended.

## requirements
This script requires:
* a working PostgreSQL database reachable on the machine where it is executed
* admin credentials for this database
* access key/secret pair with write access to the S3 bucket
* the following apt packages: python3-psycopg2, python3-boto3, postgresql-client
  
## usage

In order to use this script the config.py variables must be edited after which the following command can be executed:
```
python3 sketch_prepare.py NENTRIES
```

where NENTRIES is the number of entries (files and database rows) to generate. The --verbose flag is available for debugging purposes.