## Introduction
This script migrates legacy data to the new desired production situation. Specifically, this script:
* copies the files from bucket ```sketch-legacy-s3/image``` to bucket ```sketch-production-s3/avatar```
* updates the corresponding database references from image/avatar-XXX.png to avatar/avatar-XXX.png

For safety reasons nothing is deleted from bucket ```sketch-legacy-s3```. The simple operation of deleting all legacy files can be done manually once it is clear that the migration process went without problems.

## requirements
This script requires:
* a working PostgreSQL database reachable on the machine where it is executed
* credentials that are enough for row UPDATES on this database
* access key/secret pair with write access to the S3 bucket
* the following apt packages: ```python3-psycopg2```, ```python3-boto3```, ```postgresql-client```
* the following environment variables: ```SKETCH_DB_USER```, ```SKETCH_DB_PASS```, ```AWS_ACCESS_KEY_ID```, ```AWS_SECRET_ACCESS_KEY```
  
## usage

In order to use this script the [config.py](lib/config.py) variables must be edited after which the following command can be executed:
```
usage: sketch_migrate.py [-h] [-p PARALLELIZATION_LEVEL] [-b BATCH_SIZE] [-v] [-d] [-s]
```

where ```BATCH_SIZE``` is the number of legacy data entries (bucket files, database rows) that are migrated on a single iteration and ```PARALLELIZATION_LEVEL``` is the number of iterations executed in parallel. The -d value forces a dry run execution mode and the -s flag forces a data status report mode. The -v flag is available for debug purposes.

Please use ```-h``` to review the full list of options.

## usage notes

It is recommended to execute the script with the ```-v -d -p 1```, before proceeding to the actual migration. This combination of options launches a verbose single process dry run where the actions that would be executed are printed to the terminal instead. This type of execution allows for familiarization with the migration procedure without any actual impact on the data.

All the execution output that is written to the terminal is also saved on a unique log file the location of which the script shows to the user.

