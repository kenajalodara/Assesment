# import necessary libraries
import glob
import os
import pickle
import pandas as pd


# Allow User to enter the path of their csv files location
def create_combined_file(path):
    """
    Creates Combined.csv file in the provided path
    path: path to module where files are to be processed
    """
    with open(path + 'Combined.csv', 'w+') as combined:
        combined.write('Source IP,Environment' + '\n')


class ProcessIpAddresses:
    """
    Takes a path where Ip address csv files will be processed and generates a combined.csv file
    """

    def __init__(self, path=None):
        if not path:
            path = input("Enter path to processed:")
        # File path for the folder to be monitored
        self.path = path
        # Check if a Combined.csv is already present if not create
        # State lookup and retrieve list of unprocessed files in the path
        self.combined_csv_path = self.path + 'Combined.csv'

    def process_files(self):
        """
        Go through unprocessed files and update the Combined.csv file with the new data added
        combined_csv_path: Path to combined csv file
        unprocessed_files: List of files that are new/ pending processing
        """

        if self.combined_csv_path not in glob.glob(self.path + "/*.csv"):
            create_combined_file(self.path)

        unprocessed_files = self.check_new_files(self.path)
        combined_df = pd.read_csv(self.combined_csv_path)

        for file in unprocessed_files:
            file_name = file.split('/')[-1].split('.csv')[0]
            environment = ' '.join(file_name.split(' ')[:-1]) if file_name.split(' ')[-1].isdigit() else file_name
            unprocessed_df = pd.read_csv(file, sep=',', usecols=['Source IP'])
            unprocessed_df['Environment'] = environment  # Getting Environemnt name
            combined_df = pd.concat([unprocessed_df, combined_df]).drop_duplicates().reset_index(drop=True)
        combined_df.to_csv(self.combined_csv_path, index=False, mode='w')
        print('Updating state')
        self.update_state(unprocessed_files)

    def update_state(self, files: set):
        """
        Saves the state to a pickle file by updating the pickle with newly processed files in a set object.
        files: List of files that are processed in this run and needs to be saved to state
        """
        try:
            with open(self.path + 'processed_files.pickle', 'rb') as f_pickle:
                processed_files = pickle.load(f_pickle)

            processed_files = processed_files.union(files)

            with open(self.path + 'processed_files.pickle', 'wb') as f_pickle:
                pickle.dump(processed_files, f_pickle)
        except Exception as error:
            print('Error while updating state: ' + repr(error))

    def check_new_files(self, path):
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
            pickle.dump(processed_files, open(self.path + 'processed_files.pickle', 'wb'))
        all_csv_files = set(glob.glob(path + "/*.csv"))
        unprocessed_files = all_csv_files.difference(processed_files)
        print('Files to be processed : ', unprocessed_files)
        return unprocessed_files
