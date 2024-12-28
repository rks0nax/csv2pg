from typing import Any
import pandas as pd
import csv
import json
import os


# Function to log errors to a CSV file
def log_error(error_file: str, row: pd.Series):
    with open(error_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(row.tolist())

def save_checkpoint(file_path: str, checkpoint: int, schema: str, table: str, columns: list[tuple[str, str]]):
    checkpoint_data: dict[str, Any] = {
        'checkpoint': checkpoint,
        'schema': schema,
        'table': table,
        'columns': columns
    }
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(f"{json.dumps(checkpoint_data)}\n")

def _load_checkpoint(file_path: str) -> tuple[int, str, str, list[str], bool]:
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = json.loads(f.read())
        except json.JSONDecodeError:
            return -1, '', '', [], False
        return data['checkpoint'], data['schema'], data['table'], data['columns'], True

def load_and_confirm_checkpoint(checkpoint_file: str) -> tuple[str, str, list[tuple[str, str]], int, bool]:
    """
    Loads the checkpoint file and confirms with the user if they want to continue with the saved state.

    Args:
        checkpoint_file (str): The path to the checkpoint file.

    Returns:
        tuple: A tuple containing:
            - str: The schema name.
            - str: The table name.
            - list[tuple[str, str]]: A list of tuples where each tuple contains a column name and an empty string.
            - int: The last checkpoint value.
            - bool: A flag indicating whether the checkpoint is valid and confirmed by the user.
    """
    if os.path.exists(checkpoint_file):
        last_checkpoint, saved_schema, saved_table, saved_columns, status = _load_checkpoint(checkpoint_file)
        if not status:
            print(f"Error: Invalid checkpoint file {checkpoint_file}")
            return "", "", [], -1, False
        return saved_schema, saved_table, [(col[0], col[1]) for col in saved_columns], last_checkpoint, True
    return "", "", [], -1, False