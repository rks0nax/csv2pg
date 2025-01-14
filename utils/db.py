from typing import Any


def generate_query_string(rows: list[list[Any]], selected_columns: list[tuple[str, str]], table: str, schema: str) -> str|None:
    """
    Generates an SQL INSERT query string for the given data, table, and schema.

    Args:
        rows (list[list[Any]]): A list of data rows, where each row is a list of values corresponding to the selected columns.
        selected_columns (list[tuple[str, str]]): A list of tuples where each tuple contains a column name and its data type.
        table (str): The name of the table.
        schema (str): The name of the schema.

    Returns:
        str: The generated SQL INSERT query string.
    """
    if not rows:
        return None
    query = f"INSERT INTO {schema}.{table} ("
    for column, _ in selected_columns:
        query += f"{column},"
    query = query[:-1] + ") VALUES "
    for row in rows:
        query += "("
        for val in row:
            if val is None:
                val = "NULL"
            elif isinstance(val, str):
                if val.startswith("'") and val.endswith("'"):
                    val = f"'{val[1:-1].replace("'", "''")}'" # Escape single quotes
                else:
                    val = val.replace("'", "''")
            query += f"{val},"
        query = query[:-1] + "),"
    query = query[:-1] + ";"
    return query

def generate_row(row, selected_columns: list[tuple[str, str]]) -> list[Any]:
    """
    Generate a row of data from a row based on selected columns.

    Args:
      row (dict): A dictionary representing a row of data with column names as keys.
      selected_columns (list[tuple[str, str]]): A list of tuples where each tuple contains a column name and its data type.

    Returns:
      list[Any]: A list of values corresponding to the selected columns in the row.
    """

    data: list[Any] = []
    for column, dtype in selected_columns:
        if column not in row or row[column] in [None, 'NULL']:
            data.append(None)
        elif dtype == 'bigint' or dtype == 'integer':
            data.append(int(row[column]))
        elif isinstance(row[column], int):
            data.append(row[column])
        elif type(row[column] ) == float and row[column] % 1 == 0:
            data.append(int(row[column]))
        else:
            data.append(f"'{row[column]}'")
    return data