import pandas as pd
import click
import survey
from dotenv import load_dotenv
import os

from db import get_all_schemas, get_all_tables, get_all_columns, get_db, truncate_table
from utils.db import generate_query_string, generate_data_pairs
from utils.cli import make_bold
from utils.file import load_and_confirm_checkpoint, log_error, save_checkpoint

load_dotenv()

def get_db_insert_meta() -> tuple[str, str, list[tuple[str, str]]]:
    """
    Prompts the user to select a database schema, table, and retrieves the columns of the selected table.

    This function interacts with the user to select a schema and table from the database, and then fetches
    the columns of the selected table. It uses the `get_db` context manager to establish a database connection,
    and the `survey.routines.select` method to prompt the user for selections.

    Returns:
        tuple[str, str, str]: A tuple containing the selected schema name, table name, and a list of column names.
    """
    with get_db() as conn:
        schemas = get_all_schemas(conn)
        schema_index: int = survey.routines.select('Select schema: ', options=[schema[0] for schema in schemas])
        schema = schemas[schema_index][0]
        tables = get_all_tables(conn, schema)
        table_index: int = survey.routines.select('Select table: ', options=[table[0] for table in tables])
        table = tables[table_index][0]
        columns = get_all_columns(conn, schema, table)
    return schema, table, columns

def handle_checkpoint(checkpoint_file: str) -> tuple[str, str, list[tuple[str, str]], int, bool]:
    """
    Handles the checkpoint file to either load a previous state or initialize a new one.

    Args:
        checkpoint_file (str): The path to the checkpoint file.

    Returns:
        tuple: A tuple containing:
            - str: The schema name.
            - str: The table name.
            - list[tuple[str, str]]: A list of tuples where each tuple contains a column name and an empty string.
            - int: The last checkpoint value or -1 if no valid checkpoint is found.
            - bool: A flag indicating whether the checkpoint is valid and confirmed by the user.
    """
    
    saved_schema, saved_table, saved_columns, last_checkpoint, valid_checkpoint = load_and_confirm_checkpoint(checkpoint_file)
    if valid_checkpoint and survey.routines.inquire(f"Continue with schema {make_bold(saved_schema)}, table {make_bold(saved_table)}, and columns selected?", default=True):
        return saved_schema, saved_table, saved_columns, last_checkpoint, True
    schema, table, columns = get_db_insert_meta()
    return schema, table, columns, -1, False

def validate_and_select_columns(df: pd.DataFrame, columns: list[tuple[str, str]]) -> list[tuple[str, str]]:
        """
        Validates the schema, table, and columns, and allows the user to select/deselect columns to insert.

        Args:
            df (pd.DataFrame): The DataFrame containing the CSV data.
            columns (list[tuple[str, str]]): The list of columns in the table.

        Returns:
            list[tuple[str, str]]: The list of selected columns to insert.
        """
        csv_columns = df.columns
        table_columns = [column[0] for column in columns]
        missing_columns = set(table_columns) - set(csv_columns)
        extra_columns = set(csv_columns) - set(table_columns)
        
        if missing_columns:
            print(f"Found {len(missing_columns)} Missing columns in CSV file:")
            print(f"{' | '.join([make_bold(col) for col in missing_columns])}")
        
        if extra_columns:
            print(f"Found {len(extra_columns)} Extra columns in CSV file:")
            print(f"{' | '.join([make_bold(col) for col in extra_columns])}")
        
        # Generate a list of column names to insert
        to_insert_columns = list(set(table_columns) - set(missing_columns))
        selected_columns_index: list[int] = survey.routines.basket('Select columns to insert: ', options=to_insert_columns, active=range(len(to_insert_columns)))
        selected_columns = [to_insert_columns[i] for i in selected_columns_index]
        return [column for column in columns if column[0] in selected_columns]

def validate_csv_columns(df: pd.DataFrame, columns: list[tuple[str, str]]) -> bool:
    """
    Check if the columns passed are all part of the CSV file. Returns True if columns is a subset of the CSV columns.

    Args:
        df (pd.DataFrame): The DataFrame containing the CSV data.
        columns (list[tuple[str, str]]): The list of columns in the table.

    Returns:
        bool: True if the columns are valid, False otherwise.
    """
    csv_columns = df.columns
    table_columns = [column[0] for column in columns]
    return set(table_columns).issubset(set(csv_columns))

@click.command()
@click.option('-f', '--file', 'csv_file', type=str, help='CSV file path', required=True)
@click.option('--clear-table', 'clear_table', is_flag=True, help='Clear the table before inserting data')
@click.option('--checkpoint-file', 'checkpoint_file', type=str, default=None, help='Checkpoint file path')
@click.option('--error-file', 'error_file', type=str, default=None, help='Error log file path')
def main(csv_file: str, clear_table: bool, checkpoint_file: str|None, error_file: str|None):
    checkpoint_file = checkpoint_file or f"{os.path.splitext(csv_file)[0]}_checkpoint.txt"
    error_file = error_file or f"{os.path.splitext(csv_file)[0]}_errors.csv"

    # Step 1: Get the schema, table, and columns to insert
    schema, table, columns, last_checkpoint, is_checkpoint = handle_checkpoint(checkpoint_file)
    if is_checkpoint and clear_table:
        print(f"Error: Cannot clear table when using a checkpoint file")
        return 1

    # Step 2: Read the CSV file
    try:
        df: pd.DataFrame = pd.read_csv(csv_file) # type: ignore
    except FileNotFoundError:
        print(f"Error: File {csv_file} not found")
        return 1

    # Step 3 and Step 4: Validate the schema, table, and columns, and select/deselect columns to insert
    if is_checkpoint:
        if not validate_csv_columns(df, columns):
            print("Error: Columns in the CSV file do not match the columns in the table")
            return 1
        selected_columns = columns
    else:
        selected_columns = validate_and_select_columns(df, columns)
    
    # -- Confirm the columns to insert
    if not is_checkpoint and not survey.routines.inquire(f'Do you want to continue w/ table {make_bold(table)} in schema {make_bold(schema)}? ', default=False):
        print('Exiting...')
        return 1
    
    # Step 5: Insert data into the table
    # -- Truncate the table if the flag is set
    if clear_table:
        print(f"Truncating table {make_bold(table)} in schema {make_bold(schema)}")
        with get_db() as conn:
            truncate_table(conn, schema, table)
    
    # -- Ensure the data is in the correct order and handle missing values
    df: pd.DataFrame = df.where(pd.notnull(df), None)
    # -- Replace NaN values with 'NULL' for the SQL query
    df: pd.DataFrame = df.fillna('NULL')

    progress = survey.graphics.MultiLineProgressControl(df.shape[0], value=last_checkpoint + 1, color = survey.colors.basic('blue' ))
    with survey.graphics.MultiLineProgress([progress], prefix = 'Inserting '):
        with get_db() as conn:
            cursor = conn.cursor()
            for index, row in df.iterrows():
                if index <= last_checkpoint:
                    continue  # Skip rows up to the last checkpoint
                
                try:
                    data = generate_data_pairs(row, selected_columns)
                    query = generate_query_string(data, table, schema)
                    if query is None:
                        print(f"Skipping row {index}: No data to insert")
                        continue
                    cursor.execute(query)
                    progress.move(1) # type: ignore
                    # Commit every 100 rows
                    if (index + 1) % 100 == 0:
                        conn.commit()
                        save_checkpoint(checkpoint_file, index, schema, table, selected_columns)
                except Exception as e:
                    print(f"Error inserting row {index}: {e}")
                    log_error(error_file, row)
                    continue

            conn.commit()
            cursor.close()
            save_checkpoint(checkpoint_file, index, schema, table, selected_columns)

if __name__ == '__main__':
    main()