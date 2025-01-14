import pytest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from main import get_db_insert_meta, handle_checkpoint, validate_and_select_columns, validate_csv_columns, main

class TestGetDbInsertMeta:
    @patch('main.get_db')
    @patch('main.get_all_schemas')
    @patch('main.get_all_tables')
    @patch('main.get_all_columns')
    @patch('survey.routines.select')
    def test_get_db_insert_meta(self, mock_select, mock_get_all_columns, mock_get_all_tables, mock_get_all_schemas, mock_get_db):
        mock_conn = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_conn
        mock_get_all_schemas.return_value = [('public',), ('test_schema',)]
        mock_get_all_tables.return_value = [('test_table',)]
        mock_get_all_columns.return_value = [('column1', 'text'), ('column2', 'integer')]

        mock_select.side_effect = [1, 0]  # Select 'test_schema' and 'test_table'

        schema, table, columns = get_db_insert_meta(None, None)

        assert schema == 'test_schema'
        assert table == 'test_table'
        assert columns == [('column1', 'text'), ('column2', 'integer')]

    @patch('main.get_db')
    @patch('main.get_all_schemas')
    @patch('main.get_all_tables')
    @patch('main.get_all_columns')
    def test_get_db_insert_meta_with_provided_schema_and_table(self, mock_get_all_columns, mock_get_all_tables, mock_get_all_schemas, mock_get_db):
        mock_conn = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_conn
        mock_get_all_schemas.return_value = [('public',), ('test_schema',)]
        mock_get_all_tables.return_value = [('test_table',)]
        mock_get_all_columns.return_value = [('column1', 'text'), ('column2', 'integer')]

        schema, table, columns = get_db_insert_meta('test_schema', 'test_table')

        assert schema == 'test_schema'
        assert table == 'test_table'
        assert columns == [('column1', 'text'), ('column2', 'integer')]

class TestHandleCheckpoint:
    @patch('main.load_and_confirm_checkpoint')
    @patch('main.get_db_insert_meta')
    @patch('survey.routines.inquire')
    def test_handle_checkpoint_with_valid_checkpoint(self, mock_inquire, mock_get_db_insert_meta, mock_load_and_confirm_checkpoint):
        mock_load_and_confirm_checkpoint.return_value = ('test_schema', 'test_table', [('column1', 'text')], 100, True)
        mock_inquire.return_value = True

        schema, table, columns, last_checkpoint, valid_checkpoint = handle_checkpoint('checkpoint_file', None, None)

        assert schema == 'test_schema'
        assert table == 'test_table'
        assert columns == [('column1', 'text')]
        assert last_checkpoint == 100
        assert valid_checkpoint == True

    @patch('main.load_and_confirm_checkpoint')
    @patch('main.get_db_insert_meta')
    def test_handle_checkpoint_with_invalid_checkpoint(self, mock_get_db_insert_meta, mock_load_and_confirm_checkpoint):
        mock_load_and_confirm_checkpoint.return_value = ('', '', [], -1, False)
        mock_get_db_insert_meta.return_value = ('test_schema', 'test_table', [('column1', 'text')])

        schema, table, columns, last_checkpoint, valid_checkpoint = handle_checkpoint('checkpoint_file', None, None)

        assert schema == 'test_schema'
        assert table == 'test_table'
        assert columns == [('column1', 'text')]
        assert last_checkpoint == -1
        assert valid_checkpoint == False

class TestValidateAndSelectColumns:
    @patch('survey.routines.basket')
    def test_validate_and_select_columns(self, mock_basket):
        df = pd.DataFrame({'column1': [1, 2], 'column2': [3, 4], 'column3': [5, 6]})
        columns = [('column1', 'integer'), ('column2', 'integer'), ('column4', 'integer')]

        mock_basket.return_value = [0, 1]  # Select 'column1' and 'column2'

        selected_columns = validate_and_select_columns(df, columns)

        assert selected_columns == [('column1', 'integer'), ('column2', 'integer')]

class TestValidateCsvColumns:
    def test_validate_csv_columns(self):
        df = pd.DataFrame({'column1': [1, 2], 'column2': [3, 4]})
        columns = [('column1', 'integer'), ('column2', 'integer')]

        assert validate_csv_columns(df, columns) == True

    def test_validate_csv_columns_with_missing_columns(self):
        df = pd.DataFrame({'column1': [1, 2]})
        columns = [('column1', 'integer'), ('column2', 'integer')]

        assert validate_csv_columns(df, columns) == False

class TestMain:
    @patch('main.handle_checkpoint')
    @patch('main.pd.read_csv')
    @patch('main.validate_csv_columns')
    @patch('main.validate_and_select_columns')
    @patch('main.survey.routines.inquire')
    @patch('main.truncate_table')
    @patch('main.get_db')
    @patch('main.generate_query_string')
    @patch('main.generate_row')
    @patch('main.save_checkpoint')
    def test_main(self, mock_save_checkpoint, mock_generate_row, mock_generate_query_string, mock_get_db, mock_truncate_table, mock_inquire, mock_validate_and_select_columns, mock_validate_csv_columns, mock_read_csv, mock_handle_checkpoint):
        mock_handle_checkpoint.return_value = ('test_schema', 'test_table', [('column1', 'integer')], -1, False)
        mock_read_csv.return_value = pd.DataFrame({'column1': [1, 2], 'column2': [3, 4]})
        mock_validate_csv_columns.return_value = True
        mock_validate_and_select_columns.return_value = [('column1', 'integer')]
        mock_inquire.return_value = True
        mock_generate_row.side_effect = [[1], [2]]
        mock_generate_query_string.return_value = 'INSERT INTO test_schema.test_table (column1) VALUES (1),(2);'

        mock_conn = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_conn

        main(['--file', 'test.csv', '--schema', 'test_schema', '--table', 'test_table'])

        mock_handle_checkpoint.assert_called_once_with('test.csv_checkpoint.txt', 'test_schema', 'test_table')
        mock_read_csv.assert_called_once_with('test.csv')
        mock_validate_csv_columns.assert_called_once_with(mock_read_csv.return_value, [('column1', 'integer')])
        mock_validate_and_select_columns.assert_called_once_with(mock_read_csv.return_value, [('column1', 'integer')])
        mock_inquire.assert_called_once_with('Do you want to continue w/ table \x1b[1mtest_table\x1b[0m in schema \x1b[1mtest_schema\x1b[0m? ', default=False)
        mock_truncate_table.assert_not_called()
        mock_generate_row.assert_has_calls([call(mock_read_csv.return_value.iloc[0], [('column1', 'integer')]), call(mock_read_csv.return_value.iloc[1], [('column1', 'integer')])])
        mock_generate_query_string.assert_called_once_with([[1], [2]], [('column1', 'integer')], 'test_table', 'test_schema')
        mock_conn.cursor.return_value.execute.assert_called_once_with('INSERT INTO test_schema.test_table (column1) VALUES (1),(2);')
        mock_save_checkpoint.assert_called_once_with('test.csv_checkpoint.txt', 1, 'test_schema', 'test_table', [('column1', 'integer')])
