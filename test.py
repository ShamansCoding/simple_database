import sys
import unittest

try:
    from StringIO import StringIO
except ModuleNotFoundError:
    from io import StringIO
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from data_base import DataBase, call_method, main, HELP


class MockForTest(object):

    def some_method(self, args):

        return 'success'


class DataBaseTest(unittest.TestCase):

    def setUp(self):

        self.database = DataBase()

    def test_set(self):

        self.database.SET('A', '10')

        self.assertEqual(self.database._storage, {'A': '10'})

    def test_set_twice(self):

        self.database.SET('A', '10')
        self.database.SET('A', '10')

        self.assertEqual(self.database._storage, {'A': '10'})

    def test_set_two_values(self):

        self.database.SET('A', '10')
        self.database.SET('B', '10')

        self.assertEqual(self.database._storage, {'A': '10', 'B': '10'})

    def test_set_in_transaction(self):
        self.database._storage = {'A': '10'}
        self.database._transaction_number = 1
        self.database._rollback_cache[1] = []
        self.database.SET('B', '20')

        self.assertEqual(self.database._storage, {'A': '10', 'B': '20'})
        self.assertEqual(self.database._rollback_cache,
                         {1: [('UNSET', 'B')]})

    def test_set_in_transaction_twice(self):
        self.database._storage = {'A': '10'}
        self.database._transaction_number = 1
        self.database._rollback_cache[1] = []
        self.database.SET('B', '20')
        self.database.SET('B', '20')

        self.assertEqual(self.database._storage, {'A': '10', 'B': '20'})
        self.assertEqual(self.database._rollback_cache,
                         {1: [('UNSET', 'B'), ('SET', 'B', '20')]})

    def test_set_in_transaction_rolling_back(self):
        self.database._storage = {'A': '10'}
        self.database._transaction_number = 1
        self.database._rollback_cache[1] = []
        self.database._rolling_back = True
        self.database.SET('B', '20')

        self.assertEqual(self.database._storage, {'A': '10', 'B': '20'})
        self.assertEqual(self.database._rollback_cache, {1: []})

    def test_get(self):
        self.database._storage = {'A': '10'}
        capturedOutput = StringIO()
        sys.stdout = capturedOutput
        self.database.GET('A')
        sys.stdout = sys.__stdout__

        self.assertEqual(capturedOutput.getvalue(), '10\n')

    def test_get_null(self):
        capturedOutput = StringIO()
        sys.stdout = capturedOutput
        self.database.GET('A')
        sys.stdout = sys.__stdout__

        self.assertEqual(capturedOutput.getvalue(), 'NULL\n')

    def test_unset(self):
        self.database._storage = {'A': '10'}
        self.database.UNSET('A')

        self.assertEqual(self.database._storage, {})

    def test_unset_no_key(self):
        self.database._storage = {}
        self.database.UNSET('A')

        self.assertEqual(self.database._storage, {})

    def test_unset_in_transaction(self):
        self.database._storage = {'A': '10'}
        self.database._transaction_number = 1
        self.database._rollback_cache[1] = []
        self.database.UNSET('A')

        self.assertEqual(self.database._storage, {})
        self.assertEqual(self.database._rollback_cache,
                         {1: [('SET', 'A', '10')]})

    def test_unset_in_transaction_twice(self):
        self.database._storage = {'A': '10'}
        self.database._transaction_number = 1
        self.database._rollback_cache[1] = []
        self.database.UNSET('A')
        self.database.UNSET('A')

        self.assertEqual(self.database._storage, {})
        self.assertEqual(self.database._rollback_cache,
                         {1: [('SET', 'A', '10')]})

    def test_unset_in_transaction_rolling_back(self):
        self.database._storage = {'A': '10'}
        self.database._transaction_number = 1
        self.database._rollback_cache[1] = []
        self.database._rolling_back = True
        self.database.UNSET('A')

        self.assertEqual(self.database._storage, {})
        self.assertEqual(self.database._rollback_cache, {1: []})

    def test_counts(self):
        self.database._storage = {'A': '10'}
        capturedOutput = StringIO()
        sys.stdout = capturedOutput

        self.database.COUNTS('10')

        sys.stdout = sys.__stdout__

        self.assertEqual(capturedOutput.getvalue(), '1\n')

    def test_counts_no_value(self):
        self.database._storage = {'A': '11'}
        capturedOutput = StringIO()
        sys.stdout = capturedOutput

        self.database.COUNTS('10')

        sys.stdout = sys.__stdout__

        self.assertEqual(capturedOutput.getvalue(), '0\n')

    def test_counts_two_values(self):
        self.database._storage = {'A': '10', 'B': '10'}
        capturedOutput = StringIO()
        sys.stdout = capturedOutput

        self.database.COUNTS('10')

        sys.stdout = sys.__stdout__

        self.assertEqual(capturedOutput.getvalue(), '2\n')

    def test_find(self):
        self.database._storage = {'A': '10'}
        capturedOutput = StringIO()
        sys.stdout = capturedOutput

        self.database.FIND('10')

        sys.stdout = sys.__stdout__

        self.assertEqual(capturedOutput.getvalue(), 'A\n')

    def test_find_no_value(self):
        self.database._storage = {'A': '11'}
        capturedOutput = StringIO()
        sys.stdout = capturedOutput

        self.database.FIND('10')

        sys.stdout = sys.__stdout__

        self.assertEqual(capturedOutput.getvalue(), '\n')

    def test_find_two_values(self):
        self.database._storage = {'A': '10', 'B': '10'}
        capturedOutput = StringIO()
        sys.stdout = capturedOutput

        self.database.FIND('10')

        sys.stdout = sys.__stdout__

        self.assertEqual(capturedOutput.getvalue(), 'A B\n')

    def test_begin(self):
        self.assertEqual(self.database._transaction_number, 0)
        self.assertEqual(self.database._rollback_cache, {})

        self.database.BEGIN()

        self.assertEqual(self.database._transaction_number, 1)
        self.assertEqual(self.database._rollback_cache, {1: []})

    def test_begin_twice(self):
        self.assertEqual(self.database._transaction_number, 0)
        self.assertEqual(self.database._rollback_cache, {})

        self.database.BEGIN()

        self.assertEqual(self.database._transaction_number, 1)
        self.assertEqual(self.database._rollback_cache, {1: []})

        self.database.BEGIN()

        self.assertEqual(self.database._transaction_number, 2)
        self.assertEqual(self.database._rollback_cache, {1: [], 2: []})

    def test_rollback(self):
        self.assertEqual(self.database._transaction_number, 0)
        self.assertEqual(self.database._rollback_cache, {})
        self.assertEqual(self.database._rolling_back, False)

        self.database._transaction_number = 1
        self.database._storage = {'A': '10', 'B': '10'}
        self.database._rollback_cache = {1: [('UNSET', 'B')]}

        self.database.ROLLBACK()

        self.assertEqual(self.database._transaction_number, 0)
        self.assertEqual(self.database._rollback_cache, {})
        self.assertEqual(self.database._rolling_back, False)
        self.assertEqual(self.database._storage, {'A': '10'})

    def test_rollback_same_key_edited_twice(self):
        self.assertEqual(self.database._transaction_number, 0)
        self.assertEqual(self.database._rollback_cache, {})
        self.assertEqual(self.database._rolling_back, False)

        self.database._transaction_number = 1
        self.database._storage = {'A': '10'}
        self.database._rollback_cache = {1: [('UNSET', 'B'),
                                             ('SET', 'B', '10')]}

        self.database.ROLLBACK()

        self.assertEqual(self.database._transaction_number, 0)
        self.assertEqual(self.database._rollback_cache, {})
        self.assertEqual(self.database._rolling_back, False)
        self.assertEqual(self.database._storage, {'A': '10'})

    def test_rollback_nested(self):
        self.assertEqual(self.database._transaction_number, 0)
        self.assertEqual(self.database._rollback_cache, {})
        self.assertEqual(self.database._rolling_back, False)

        self.database._transaction_number = 2
        self.database._storage = {'A': '10', 'B': '10'}
        self.database._rollback_cache = {1: [('UNSET', 'B')],
                                         2: [('SET', 'C', '3')]}

        self.database.ROLLBACK()

        self.assertEqual(self.database._transaction_number, 1)
        self.assertEqual(self.database._rollback_cache, {1: [('UNSET', 'B')]})
        self.assertEqual(self.database._rolling_back, False)
        self.assertEqual(self.database._storage,
                         {'A': '10', 'C': '3', 'B': '10'})

    def test_commit(self):

        self.database._transaction_number = 1
        self.database._rollback_cache = {1: [('UNSET', 'B')]}

        self.database.COMMIT()

        self.assertEqual(self.database._transaction_number, 0)
        self.assertEqual(self.database._rollback_cache, {})


class CallMethodTest(unittest.TestCase):

    def setUp(self):
        self.test_instance = MockForTest()

    def test_call_method(self):
        result = call_method(self.test_instance, ['some_method', '1'])

        self.assertTrue(result)

    def test_call_method_invalid_name(self):
        capturedOutput = StringIO()
        sys.stdout = capturedOutput
        result = call_method(self.test_instance, ['invalid_method', '1'])
        self.assertFalse(result)

        sys.stdout = sys.__stdout__

        self.assertEqual(
            capturedOutput.getvalue(),
            ' -> Invalid method name. To get methods names type HELP.\n'
        )

    def test_call_method_invalid_args_number(self):
        capturedOutput = StringIO()
        sys.stdout = capturedOutput
        result = call_method(self.test_instance, ['some_method'])
        self.assertFalse(result)

        sys.stdout = sys.__stdout__

        self.assertEqual(
            capturedOutput.getvalue(),
            ' -> Invalid number of argumets for some_method\n'
        )


class MainTest(unittest.TestCase):

    @patch('data_base.call_method')
    @patch('data_base.input')
    def test_main(self, mock_input, mock_call_method):

        mock_input.side_effect = ['METHOD', 'END']
        mock_call_method.return_value = sys.exit

        capturedOutput = StringIO()
        sys.stdout = capturedOutput

        main()

        sys.stdout = sys.__stdout__

        self.assertEqual(
            capturedOutput.getvalue(),
            ('Welcome to simple database! Type HELP for help.\n'
             'Exiting database...\n')
        )

    @patch('data_base.call_method')
    @patch('data_base.input')
    def test_main_help(self, mock_input, mock_call_method):

        mock_input.side_effect = ['HELP', 'END']
        mock_call_method.return_value = sys.exit

        capturedOutput = StringIO()
        sys.stdout = capturedOutput

        main()

        sys.stdout = sys.__stdout__

        self.assertEqual(
            capturedOutput.getvalue(),
            ('Welcome to simple database! Type HELP for help.\n' +
             HELP + '\n' +
             'Exiting database...\n')
        )


if __name__ == '__main__':
    unittest.main()
