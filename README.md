# csv2pg

This script reads CSV file and inserts the data into a specified table in a database.

## Requirements

- Poetry (https://python-poetry.org)

## Installation

1. Clone the repository
2. Install Poetry:
    ```sh
    curl -sSL https://install.python-poetry.org | python3 -
    ```
    Read install instruction here: [Poetry installation Link](https://python-poetry.org/docs/#installing-with-the-official-installer)
3. Install the required Python packages using Poetry:
    ```sh
    poetry install
    ```
4. Create a `.env` file in the root directory and add your database connection details.
    ```sh
    DB_HOST=your_host
    DB_PORT=your_port
    DB_NAME=your_database
    DB_USER=your_user
    DB_PASSWORD=your_password
    ```

## Usage

To run the script, use the following command:

```sh
python main.py -f <csv_file_path> [--clear-table] [--checkpoint-file <checkpoint_file_path>] [--error-file <error_file_path>]
```

### Example

```sh
python main.py -f data.csv --clear-table --checkpoint-file checkpoint.txt --error-file errors.csv
```

### Options

- `-f, --file`: Path to the CSV file (required).
- `--clear-table`: Clear the table before inserting data (optional).
- `--checkpoint-file`: Path to the checkpoint file to resume from a previous state (optional).
- `--error-file`: Path to the error log file to log rows that failed to insert (optional).

## Functionality

1. The script connects to the database and retrieves all schemas and tables.
2. It allows the user to select a schema and a table.
3. It reads the structure of the selected table.
4. It reads the CSV file and checks if the columns match the table structure.
5. It allows the user to select/deselect columns to insert.
6. It inserts the data into the table, optionally truncating the table first.
7. It supports resuming from a checkpoint file to continue from where it left off.
8. It logs any rows that fail to insert into an error log file.

## License

This project is licensed under the MIT License.