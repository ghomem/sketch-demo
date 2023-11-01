## Introduction
This script is an integration test. Specificallyt, it executes a cycle of environment preparation + data migration and evaluates the correctness of the result, while measuring the elapsed time for the migration part.

**WARNING** To ensure clean execution conditions, this script deletes previously existing data in the database in the legacy bucket and in the production bucket. Do not execute if this is not intended. Do not execute this on a production environment.

## requirements
This script requires:
* a working PostgreSQL database reachable from the machine where it is executed
* admin credentials for this database
* access key/secret pair with write access to the S3 bucket
* the following apt packages: ```python3-psycopg2```, ```python3-boto3```, ```postgresql-client```
* the following environment variables: ```SKETCH_DB_USER```, ```SKETCH_DB_PASS```, ```AWS_ACCESS_KEY_ID```, ```AWS_SECRET_ACCESS_KEY```
  
## usage

```
python3 sketch_test.py number_of_avatars batch_size parallelization_level
```

where ```number_of_avatars``` is the number of entries (files and database rows) to generate on the simulated environment, ```batch_size``` is the number of legacy data entries (bucket files, database rows) that are migrated on a single iteration and ```parallelization_level``` is the number of iterations executed in parallel.
