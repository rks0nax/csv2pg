from typing import Any


def generate_query_string(data: list[tuple[str, Any]], table: str, schema: str) -> str|None:
    """
    Generates an SQL INSERT query string for the given data, table, and schema.

    Args:
      data (list[tuple[str, str]]): A list of tuples where each tuple contains a column name and its corresponding value.
      table (str): The name of the table to insert data into.
      schema (str): The name of the schema to which the table belongs.

    Returns:
      str: The generated SQL INSERT query string.
    """
    if not data:
        return None
    query = f"INSERT INTO {schema}.{table} ("
    for column, _ in data:
        query += f"{column},"
    query = query[:-1] + ") VALUES ("
    for _, val in data:
        if isinstance(val, str):
            if val.startswith("'") and val.endswith("'"):
                val = f"'{val[1:-1].replace("'", "''")}'" # Escape single quotes in string values
            else:
                val = val.replace("'", "''")  # Escape single quotes in string values
        query += f"{val},"
    query = query[:-1] + ");"
    return query

def generate_data_pairs(row, selected_columns: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """
    Generate a list of data pairs from a row based on selected columns and their data types.

    Args:
      row (dict): A dictionary representing a row of data with column names as keys.
      selected_columns (list[tuple[str, str]]): A list of tuples where each tuple contains a column name and its data type.

    Returns:
      list[tuple[str, str]]: A list of tuples where each tuple contains a column name and its corresponding value, 
                   formatted according to its data type.
    """
    data: list[tuple[str, str]] = []
    for column, dtype in selected_columns:
        if column not in row or row[column] in [None, 'NULL']:
            continue
        if dtype == 'bigint' or dtype == 'integer':
            data.append((column, int(row[column])))
        elif isinstance(row[column], int):
            data.append((column, row[column]))
        elif type(row[column] ) == float and row[column] % 1 == 0:
            data.append((column, int(row[column])))
        else:
            data.append((column, f"'{row[column]}'"))
    return data
