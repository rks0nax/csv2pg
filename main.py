import pandas as pd
import click
import survey
from dotenv import load_dotenv

from db import get_all_schemas, get_all_tables, get_db, truncate_table
from utils.cli import make_bold

load_dotenv()

def generate_query_strings(data: list[tuple[str, str]], table: str, schema: str) -> str:
    query = f"INSERT INTO {schema}.{table} ("
    for column, _ in data:
        query += f"{column},"
    query = query[:-1] + ") VALUES ("
    for _, val in data:
        query += f"{val},"
    query = query[:-1] + ");"
    return query

def generate_data_pairs(row, selected_columns: list[tuple[str, str]]) -> list[tuple[str, str]]:
    data: list[tuple[str, str]] = []
    for column, dtype in selected_columns:
        if row[column] in [None, 'NULL']:
            continue
        if dtype == 'bigint' or dtype == 'integer':
            data.append((column, int(row[column])))
        elif isinstance(row[column], int):
            data.append((column, row[column]))
        else:
            data.append((column, f"'{row[column]}'"))
    return data

@click.command()
@click.option('-f', '--file', 'csv_file', type=str, help='CSV file path', required=True)
@click.option('--clear-table', 'clear_table', is_flag=True, help='Clear the table before inserting data')
def main(csv_file: str, clear_table: bool):
    
    schema = None
    table = None
    columns = None
    # Step 1: Match table structure with CSV file
    # -- Get all schemas and tables in the database
    with get_db() as conn:
      schemas = get_all_schemas(conn)
      schema_index: int = survey.routines.select('Select schema: ', options=[schema[0] for schema in schemas])
      schema = schemas[schema_index][0]
      tables = get_all_tables(conn, schema)
      table_index: int = survey.routines.select('Select table: ', options=[table[0] for table in tables])
      table = tables[table_index][0]

    # -- Get the table structure
    with get_db() as conn:
      cursor = conn.cursor()
      cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}';")
      columns = cursor.fetchall()
      
    
    # Step 3: Read the CSV file
    try:
      df: pd.DataFrame = pd.read_csv(csv_file) # type: ignore
    except FileNotFoundError:
      print(f"Error: File {csv_file} not found")
      return 1

    # Step 4: Check the columns in the CSV file match the table structure, print the differences
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
    
    # Step 5: Select/Deselect columns to insert
    # -- Generate a list of column names to insert
    to_insert_columns = list(set(table_columns) - set(missing_columns))
    selected_columns_index: list[int] = survey.routines.basket('Select columns to insert: ', options=to_insert_columns, active=range(len(to_insert_columns)))
    selected_columns = [to_insert_columns[i] for i in selected_columns_index]
    selected_columns = [column for column in columns if column[0] in selected_columns]
    
    # -- Confirm the columns to insert
    if not survey.routines.inquire(f'Do you want to continue w/ table {make_bold(table)} in schema {make_bold(schema)}? ', default=False):
      print('Exiting...')
      return 1
    
    # Step 5: Insert data into the table
    # -- Truncate the table if the flag is set
    if clear_table:
      print(f"Truncating table {make_bold(table)} in schema {make_bold(schema)}")
      with get_db() as conn:
        truncate_table(conn, schema, table)
    
    # -- Ensure the data is in the correct order and handle missing values
    df = df.where(pd.notnull(df), None)
    # -- Replace NaN values with 'NULL' for the SQL query
    df = df.fillna('NULL')

    # -- Begin inserting the data
    print(f"\nInserting file {make_bold(csv_file)} into table {make_bold(schema)}.{make_bold(table)}")
    progress = survey.graphics.MultiLineProgressControl(df.shape[0], color = survey.colors.basic('blue' ))
    with survey.graphics.MultiLineProgress([progress], prefix = f"Inserting into {schema}.{table}"):
      with get_db() as conn:
        cursor = conn.cursor()
        for index, row in df.iterrows():
          data = generate_data_pairs(row, selected_columns)
          cursor.execute(generate_query_strings(data, table, schema))
          progress.move(1)
          # Commit every 1000 rows
          if (index + 1) % 1000 == 0:
            conn.commit()

        conn.commit()
        cursor.close()

if __name__ == '__main__':
    main()