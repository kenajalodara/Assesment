import pandas as pd

UNPROCESSED_FILE_EMPTY = set()
UNPROCESSED_FILES = set()
UNPROCESSED_FILES.add(
    '/ProcessFiles/src/Tests/Asia Prod 3.csv')
UNPROCESSED_FILES.add(
    '/ProcessFiles/src/Tests/NA Prod.csv')
PICKLE_LOAD_SET = {'Path/file1', 'Path/file2'}
PICKLE_DUMP_SET = {'Path/file1', 'Path/file2', 'Path/file3'}
PICKLE_FILE_TO_ADD = {'Path/file3'}
CAUSE_EXCEPTION = list()
EMPTY_DF = pd.DataFrame(columns=['Source IP', 'Environment'])
PROCESSED_FILES = {'Path0/processedFile.csv'}