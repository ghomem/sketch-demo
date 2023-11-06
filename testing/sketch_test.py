#!/usr/bin/env python

import argparse
import os
import time
import subprocess


E_OK  = 0
E_ERR = 1

PYTHON_CMD = '/usr/bin/python3'


# this function compares the initial status given by the migration script to what has been requested in the preparation script
def check_prep_vs_mig_initial_status(prep_legacy_avatars, prep_production_avatars, mig_initial_legacy_avatars, mig_initial_production_avatars,
                                     mig_initial_legacy_db_rows, mig_initial_production_db_rows, mig_initial_total_db_rows):

    rc = True

    if prep_legacy_avatars != mig_initial_legacy_avatars:
        rc = False
        print(f"ERROR: preparation vs initial migration s3 status mismatch on the number of legacy avatars {prep_legacy_avatars} vs {mig_initial_legacy_avatars}")

    if prep_production_avatars != mig_initial_production_avatars:
        rc = False
        print(f"ERROR: preparation vs initial migration s3 status mismatch on the number of production avatars {prep_production_avatars} vs {mig_initial_production_avatars}")

    if prep_legacy_avatars != mig_initial_legacy_db_rows:
        rc = False
        print(f"ERROR: preparation vs initial migration db status mismatch on the number of legacy avatars {prep_legacy_avatars} vs {mig_initial_legacy_db_rows}")

    if prep_production_avatars != mig_initial_production_db_rows:
        rc = False
        print(f"ERROR: preparation vs initial migration db status mismatch on the number of legacy avatars {prep_production_avatars} vs {mig_initial_production_db_rows}")

    if mig_initial_total_db_rows != (mig_initial_legacy_db_rows + mig_initial_production_db_rows):
        rc = False
        print(f"ERROR: mismatch on the total number of db rows {mig_initial_legacy_db_rows} + {mig_initial_production_db_rows} vs {mig_initial_total_db_rows}")

    return rc


# this function compares the initial status given by the migration script to the final status after the migration is performed
def check_mig_initial_vs_final_status(mig_initial_legacy_avatars, mig_initial_production_avatars,
                                      mig_initial_legacy_db_rows, mig_initial_production_db_rows, mig_initial_total_db_rows,
                                      mig_final_legacy_avatars,   mig_final_production_avatars,
                                      mig_final_legacy_db_rows,   mig_final_production_db_rows,   mig_final_total_db_rows):

    rc = True

    if mig_initial_legacy_avatars != mig_final_legacy_avatars:
        rc = False
        print('ERROR: the number of legacy avatars changed, which means something has been deleted')

    if mig_initial_production_avatars == mig_final_production_avatars:
        rc = False
        print('ERROR: the number of production avatars did not change, which means the migration had problems')

    if mig_initial_production_db_rows == mig_final_production_db_rows:
        rc = False
        print('ERROR: the number of production database rows did not change, which means the migration had problems')

    if mig_final_legacy_db_rows != 0:
        rc = False
        print('ERROR: the final number of legacy database rows is not zero, which means the migration had problems')

    if mig_final_production_db_rows != mig_final_production_avatars:
        rc = False
        print(f"ERROR: mismatch on the number of db production rows vs final number of production avatars {mig_final_production_db_rows} vs {mig_final_production_avatars}")

    if mig_final_total_db_rows != (mig_final_legacy_db_rows + mig_final_production_db_rows):
        rc = False
        print(f"ERROR: mismatch on the final total number of db rows {mig_final_legacy_db_rows} + {mig_final_production_db_rows} vs {mig_final_total_db_rows}")

    return rc


# this function transforms the tech status line from stdout into a clean list
def get_list_of_values_from_execution(output_lines):

    tech_status_str    = str(output_lines[-1]).split(' ')[1]
    tech_status_values = tech_status_str.split(',')

    # the last value needs to be cleaned from a trailling \n
    tmp_value = tech_status_values[-1]
    tech_status_values[-1] = tmp_value.split('\\n')[0]

    return tech_status_values


# main script
def main():

    parser = argparse.ArgumentParser(description='This script tests the migration and generation scripts in an integrated way')

    # mandatory arguments
    parser.add_argument('number_of_avatars',      type=int, help='Number of legacy avatars to create')
    parser.add_argument('batch_size',             type=int, help='number of db and s3 entries per iteration')
    parser.add_argument('parallelization_level',  type=int, help='number of parallel workers processes')

    args = parser.parse_args()

    # establish the path of the preparation and migration executables
    base_path = os.path.dirname(os.path.abspath(__file__))
    prep_cmd = f"{base_path}/../preparation/sketch_prepare.py"
    mig_cmd  = f"{base_path}/../migration/sketch_migrate.py"

    try:
        result = subprocess.Popen([PYTHON_CMD, prep_cmd, str(args.number_of_avatars), '-cyt'], stdout=subprocess.PIPE )

        prep_output_lines = result.stdout.readlines()

        # get the output in a clean list
        tech_status_values = get_list_of_values_from_execution(prep_output_lines)

        # obtain the values that have been allocated to legacy an production avatars
        prep_legacy_avatars     = int(tech_status_values[0])
        prep_production_avatars = int(tech_status_values[1])

    except Exception as e:
        print('Error executing the preparation command')
        exit(E_ERR)

    print('Pre-migration requested values:', tech_status_values)

    try:
        # now let's request the status with the migration script
        result = subprocess.Popen([PYTHON_CMD, mig_cmd, '-st'], stdout=subprocess.PIPE )
        mig_initial_output_lines = result.stdout.readlines()

        # get the output in a clean list
        tech_status_values = get_list_of_values_from_execution(mig_initial_output_lines)

        # obtain the values that have been detected for legacy and production avatars
        mig_initial_legacy_avatars     = int(tech_status_values[0])
        mig_initial_production_avatars = int(tech_status_values[1])
        mig_initial_legacy_db_rows     = int(tech_status_values[2])
        mig_initial_production_db_rows = int(tech_status_values[3])
        mig_initial_total_db_rows      = int(tech_status_values[4])
    except Exception as e:
        print('Error executing the pre-migration status check command')
        exit(E_ERR)

    print('Pre-migration detected values :', tech_status_values)

    # check the pre-migration situation vs the situaion request to the preparation script
    if not check_prep_vs_mig_initial_status(prep_legacy_avatars,        prep_production_avatars,
                                            mig_initial_legacy_avatars, mig_initial_production_avatars,
                                            mig_initial_legacy_db_rows, mig_initial_production_db_rows, mig_initial_total_db_rows):

        exit(E_ERR)

    # if we survived this far let's perform the migration and measure the time
    start_time = time.time()

    try:
        result = subprocess.Popen([PYTHON_CMD, mig_cmd, '-tyw', f"-b {args.batch_size}", f"-p {args.parallelization_level}"], stdout=subprocess.PIPE )
        mig_final_output_lines = result.stdout.readlines()

        end_time = time.time()

        # get the output in a clean list
        tech_status_values = get_list_of_values_from_execution(mig_final_output_lines)
        print('Post-migration detected values:', tech_status_values)

        # obtain the values that have been detected as legacy and production avatars
        mig_final_legacy_avatars     = int(tech_status_values[0])
        mig_final_production_avatars = int(tech_status_values[1])
        mig_final_legacy_db_rows     = int(tech_status_values[2])
        mig_final_production_db_rows = int(tech_status_values[3])
        mig_final_total_db_rows      = int(tech_status_values[4])
    except Exception as e:
        print('Error executing the migration command')
        exit(E_ERR)

    if not check_mig_initial_vs_final_status(mig_initial_legacy_avatars, mig_initial_production_avatars,
                                             mig_initial_legacy_db_rows, mig_initial_production_db_rows, mig_initial_total_db_rows,
                                             mig_final_legacy_avatars,   mig_final_production_avatars,
                                             mig_final_legacy_db_rows,   mig_final_production_db_rows,   mig_final_total_db_rows):
        exit(E_ERR)

    # if we survived so far it means there were no problems
    print('No problems detected!')

    elapsed_time = round(end_time - start_time, 2)

    print(f"\nMigration of mixed dataset of size {mig_initial_total_db_rows} using batch size {args.batch_size} and parallelization level {args.parallelization_level} finished after {elapsed_time} seconds")

    exit(E_OK)


# main script
if __name__ == "__main__":
    main()
