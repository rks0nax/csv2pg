from utils.db import generate_query_string, generate_row
import pandas as pd
import psycopg2
from unittest.mock import patch, MagicMock

class TestGenerateQueryString:
  def test_generate_query_string(self):
    data = [('column1', "'value1'"), ('column2', "'value2'")]
    table = 'test_table'
    schema = 'test_schema'
    expected_query = "INSERT INTO test_schema.test_table (column1,column2) VALUES ('value1','value2');"
    assert generate_query_string(data, table, schema) == expected_query

  def test_generate_query_string_with_special_characters(self):
    data = [('column1', "'value1'"), ('column2', "'value2'"), ('column3', "'value3, with comma'"), ('column4', "'value4 with \"quotes\"'")]
    table = 'test_table'
    schema = 'test_schema'
    expected_query = "INSERT INTO test_schema.test_table (column1,column2,column3,column4) VALUES ('value1','value2','value3, with comma','value4 with \"quotes\"');"
    assert generate_query_string(data, table, schema) == expected_query
  
  def test_generate_query_string_escape_single_quotes(self):
    data = [('column1', "'value1'"), ('column2', "'What's up?'")]
    table = 'test_table'
    schema = 'test_schema'
    expected_query = "INSERT INTO test_schema.test_table (column1,column2) VALUES ('value1','What''s up?');"
    assert generate_query_string(data, table, schema) == expected_query

  def test_generate_query_string_with_empty_data(self):
    data = []
    table = 'test_table'
    schema = 'test_schema'
    assert generate_query_string(data, table, schema) == None

  def test_generate_query_string_with_null_values(self):
    data = [('column1', "'value1'"), ('column2', 'NULL')]
    table = 'test_table'
    schema = 'test_schema'
    expected_query = "INSERT INTO test_schema.test_table (column1,column2) VALUES ('value1',NULL);"
    assert generate_query_string(data, table, schema) == expected_query

class TestGenerateRow:
  def test_generate_row(self):
    row = pd.Series({'column1': 'value1', 'column2': 123, 'column3': 45.0, 'column4': None})
    selected_columns = [('column1', 'text'), ('column2', 'integer'), ('column3', 'bigint'), ('column4', 'text')]
    expected_data = ['value1', 123, 45, None]
    assert generate_row(row, selected_columns) == expected_data

  def test_generate_row_with_null_values(self):
    row = pd.Series({'column1': 'value1', 'column2': None, 'column3': 'NULL'})
    selected_columns = [('column1', 'text'), ('column2', 'integer'), ('column3', 'text')]
    expected_data = ['value1', None, None]
    assert generate_row(row, selected_columns) == expected_data

  def test_generate_row_with_integer_values(self):
    row = pd.Series({'column1': 'value1', 'column2': 123, 'column3': 456.0})
    selected_columns = [('column1', 'text'), ('column2', 'integer'), ('column3', 'bigint')]
    expected_data = ['value1', 123, 456]
    assert generate_row(row, selected_columns) == expected_data

  def test_generate_row_with_mixed_values(self):
    row = pd.Series({'column1': 'value1', 'column2': 123, 'column3': 45.0, 'column4': 'value4'})
    selected_columns = [('column1', 'text'), ('column2', 'integer'), ('column3', 'bigint'), ('column4', 'text')]
    expected_data = ['value1', 123, 45, 'value4']
    assert generate_row(row, selected_columns) == expected_data

  def test_generate_row_with_empty_row(self):
    row = pd.Series({})
    selected_columns = [('column1', 'text'), ('column2', 'integer')]
    expected_data = [None, None]
    assert generate_row(row, selected_columns) == expected_data

class TestGetAllSchemas:
  @patch('psycopg2.connect')
  def test_get_all_schemas(self, mock_connect):
    from db import get_all_schemas
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [('public',), ('test_schema',)]
    result = get_all_schemas(mock_conn)
    assert result == [('test_schema',)]

class TestGetAllTables:
  @patch('psycopg2.connect')
  def test_get_all_tables(self, mock_connect):
    from db import get_all_tables
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [('table1',), ('table2',)]
    result = get_all_tables(mock_conn, 'public')
    assert result == [('table1',), ('table2',)]

class TestTruncateTable:
  @patch('psycopg2.connect')
  def test_truncate_table(self, mock_connect):
    from db import truncate_table
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return.value = mock_cursor
    truncate_table(mock_conn, 'public', 'test_table')
    mock_cursor.execute.assert_called_once_with('TRUNCATE TABLE public.test_table;')
    mock_conn.commit.assert_called_once()

class TestGetAllColumns:
  @patch('psycopg2.connect')
  def test_get_all_columns(self, mock_connect):
    from db import get_all_columns
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return.value = mock_conn
    mock_conn.cursor.return.value = mock_cursor
    mock_cursor.fetchall.return.value = [('column1', 'text'), ('column2', 'integer')]
    result = get_all_columns(mock_conn, 'public', 'test_table')
    assert result == [('column1', 'text'), ('column2', 'integer')]
