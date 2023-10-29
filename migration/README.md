## Introduction
This script migrates legacy data to the new desired production situation. Specifically, this script:
* copies the files from bucket ```sketch-legacy-s3/image``` to bucket ```sketch-production-s3/avatar```
* updates the corresponding database references from image/avatar-XXX.png to avatar/avatar-XXX.png

For safety reasons nothing is deleted from bucket ```sketch-legacy-s3```. The simple operation of deleting all legacy files can be done manually once it is clear that the migration process went without problems.

## requirements
This script requires:
* a working PostgreSQL database reachable on the machine where it is executed
* admin credentials for this database
* access key/secret pair with write access to the S3 bucket
* the following apt packages: ```python3-psycopg2```, ```python3-boto3```, ```postgresql-client```
* the following environment variables: ```SKETCH_DB_ADMIN_USER```, ```SKETCH_DB_ADMIN_PASS```, ```AWS_ACCESS_KEY_ID```, ```AWS_SECRET_ACCESS_KEY```
  
## usage

In order to use this script the config.py variables must be edited after which the following command can be executed:
```
python3 sketch_migrate.py [-h] [-v] batch_size
```

where ```batch_size``` is the number of legacy data entries (bucket files, database rows) that are migrated on a single iteration. The -v flag is available for debugging purposes.
