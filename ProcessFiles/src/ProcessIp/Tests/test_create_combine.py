import os
import unittest
import mock
import ProcessIp.Tests.test_fixtures as fixtures
import sys
sys.path.insert(0,"ProcessFiles/src/ProcessIp")
from ProcessIp.create_combined import ProcessIpAddresses


class TestProcessIpAddresses(unittest.TestCase):
    path = str(os.getcwd())+'/'
    processIp = ProcessIpAddresses(path)

    @mock.patch('ProcessIp.create_combined.ProcessIpAddresses.update_state')
    @mock.patch('pandas.DataFrame.to_csv')
    @mock.patch('pandas.read_csv')
    @mock.patch('ProcessIp.create_combined.ProcessIpAddresses.check_new_files')
    @mock.patch('pickle.dump')
    @mock.patch('pickle.load')
    def test_process_files(self, mock_pickle_load, mock_pickle_dump, mock_check_new_files, mock_pd_read_csv,
                           mock_pd_to_csv, mock_update_state):
        mock_pickle_load.return_value = fixtures.PICKLE_LOAD_SET
        mock_pickle_dump.return_value = fixtures.PICKLE_DUMP_SET
        mock_check_new_files.return_value = fixtures.UNPROCESSED_FILES
        mock_pd_read_csv.return_value = fixtures.EMPTY_DF
        mock_pd_to_csv.return_value = None
        mock_update_state.return_value = None
        self.processIp.process_files()

    @mock.patch('glob.glob')
    @mock.patch('pickle.load')
    def test_check_new_files(self, mock_pickle_load, mock_glob):
        #mock_isfile.return_value = True
        mock_pickle_load.return_value = fixtures.PROCESSED_FILES
        mock_glob.return_value = fixtures.UNPROCESSED_FILES
        unprocessed_files = self.processIp.check_new_files(self.path)
        assert unprocessed_files == fixtures.UNPROCESSED_FILES
        mock_glob.return_value = fixtures.UNPROCESSED_FILE_EMPTY
        unprocessed_files = self.processIp.check_new_files(self.path)
        assert unprocessed_files == set()

    @mock.patch('pickle.dump')
    @mock.patch('pickle.load')
    def test_update_state(self, mock_pickle_load, mock_pickle_dump):
        mock_pickle_load.return_value = fixtures.PICKLE_LOAD_SET
        mock_pickle_dump.return_value = fixtures.PICKLE_DUMP_SET
        self.processIp.update_state(fixtures.PICKLE_FILE_TO_ADD)
        self.assertRaises(Exception, self.processIp.update_state(fixtures.CAUSE_EXCEPTION))

    


if __name__ == '__main__':
    unittest.main()
