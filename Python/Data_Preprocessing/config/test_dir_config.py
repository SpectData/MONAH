'''
Tests ./parallel_run_settings_secrets.py
'''
import pytest
import Python.Data_Preprocessing.config.dir_config as prs


class TestPRS:
    '''
    Scoring Test
    '''
    computer_name = 'marriane_win'

    parallel_run_settings = prs.get_parallel_run_settings(computer_name)

    # Test case #1
    def test_all_not_none(self):
        '''
        Check that all values are not none, that is they are initiated
        :return: assertion
        '''

        parallel_run_settings = TestPRS.parallel_run_settings

        result = True

        for key, val in parallel_run_settings.items():
            if val is None:
                result = False

        assert result
