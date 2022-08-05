#importing necessary libararies
import os
import pandas as pd
from datetime import datetime 
import glob        
import re
import pickle
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


def create_combined_file():
    """
    Creates Combined.csv file in the provided path
    path: path to module where files are to be processed
    """
    path = Variable.get('path')
    with open(path + 'Combined.csv', 'w+') as combined:
        combined.write('Source IP,Environment' + '\n')


def process_files():

    # Check if a Combined.csv is already present if not create
    # State lookup and retrieve list of unprocessed files in the path
    path = Variable.get('path')
    combined_csv_path = path + 'Combined.csv'

    """
    Go through unprocessed files and update the Combined.csv file with the new data added
    combined_csv_path: Path to combined csv file
    unprocessed_files: List of files that are new/ pending processing
    """

    if combined_csv_path not in glob.glob(path + "/*.csv"):
        create_combined_file()

    unprocessed_files = check_new_files(path)
    combined_df = pd.read_csv(combined_csv_path)

    for file in unprocessed_files:

        file_name = file.split('/')[-1].split('.csv')[0]
        environment = ' '.join(file_name.split(' ')[:-1]) if file_name.split(' ')[-1].isdigit() else file_name
        unprocessed_df = pd.read_csv(file, sep=',', usecols=['Source IP'])
        unprocessed_df['Environment'] = environment  # Getting Environemnt name
        combined_df = pd.concat([unprocessed_df, combined_df]).drop_duplicates().reset_index(drop=True)
    combined_df.to_csv(combined_csv_path, index=False, mode='w')

    update_state(unprocessed_files)

def update_state(files):
    """
    Saves the state to a pickle file by updating the pickle with newly processed files in a set object.
    files: List of files that are processed in this run and needs to be saved to state
    """
    try:
        with open(path + 'processed_files.pickle', 'rb') as f_pickle:
            processed_files = pickle.load(f_pickle)

        processed_files = processed_files.union(files)

        with open(path + 'processed_files.pickle', 'wb') as f_pickle:
            pickle.dump(processed_files, f_pickle)
    except Exception as error:
        print('Error while updating state: ' + repr(error))

def check_new_files(path):
    """
    Checks for any new files in the path by comparing with the state file
    path: path to module where files are to be processed
    """
    if os.path.isfile(path + 'processed_files.pickle'):
        with open(path + 'processed_files.pickle', 'rb') as f:
            processed_files = pickle.load(f)
    else:
        processed_files = set()
        processed_files.add(path + 'Combined.csv')
        pickle.dump(processed_files, open(path + 'processed_files.pickle', 'wb'))
    all_csv_files = set(glob.glob(path + "/*.csv"))
    unprocessed_files = all_csv_files.difference(processed_files)
    print('Files to be processed : ', unprocessed_files)
    return unprocessed_files
    


# Creates Dag to Schedule the task daily
with DAG(
    dag_id='combined_csv_dag',
    max_active_runs=10,
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=7, day=29),
    catchup=False
) as dag:

    # Dummy Task to initialize the task 
    initial = DummyOperator(task_id="start")

    # Calls the main method combined_csv to combined the data
    csv_process = PythonOperator(
        task_id='combined_csv',
        python_callable= process_files
    )
    # process the task in the order from initial to csv_process
    initial >> csv_process
