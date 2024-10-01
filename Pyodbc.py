import argparse
import pyodbc
import pandas as pd
import json
import logging
import sys
# python Pyodbc.py --config info.json --query "SELECT * FROM YourTable" --output results.csv --format csv --verbose


def read_config(config_path):
    """Read database connection info from a JSON config file."""
    try:
        with open(config_path, "r") as file:
            data = json.load(file)
        server = data.get('server')
        database = data.get('database')
        username = data.get('username')
        password = data.get('password')
        driver = data.get('driver', '{ODBC Driver 17 for SQL Server}')
        if not all([server, database, username, password]):
            logging.error("Missing database connection info in config file.")
            sys.exit(1)
        return server, database, username, password, driver
    except Exception as e:
        logging.error(f'Error reading config file {config_path}: {e}')
        sys.exit(1)


def get_query(args):
    """Get SQL query from command-line arguments or prompt the user."""
    if args.query:
        return args.query
    elif args.query_file:
        try:
            with open(args.query_file, "r") as qfile:
                return qfile.read()
        except Exception as e:
            logging.error(f'Error reading query file {args.query_file}: {e}')
            sys.exit(1)
    else:
        return input("Enter SQL query to execute: ")


def create_connection(server, database, username, password, driver):
    """Create a database connection."""
    try:
        conn_str = (
            f'DRIVER={driver};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        logging.error(f'Error connecting to database: {e}')
        sys.exit(1)


def execute_query(conn, query, output_file=None, output_format='csv'):
    """Execute a SQL query and process the results."""
    try:
        query_type = query.strip().split()[0].lower()
        if query_type in ('insert', 'update', 'delete'):
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
                logging.info("Query executed successfully.")
        else:
            df = pd.read_sql_query(query, conn)
            if df.empty:
                logging.info("No data found.")
            else:
                if output_file:
                    if output_format == 'csv':
                        df.to_csv(output_file, index=False)
                    elif output_format == 'xlsx':
                        df.to_excel(output_file, index=False)
                    elif output_format == 'json':
                        df.to_json(output_file, orient='records', lines=True)
                    logging.info(f"Results saved to {output_file}")
                else:
                    print(df.to_string(index=False))
    except Exception as e:
        logging.error(f'Error executing query: {e}')


def main():
    parser = argparse.ArgumentParser(
        description='Execute a SQL query on an Azure SQL Database and process the results.')
    parser.add_argument('--config', type=str, default='info.json',
                        help='Path to the config JSON file containing database connection info.')
    parser.add_argument('--query', type=str, help='SQL query to execute.')
    parser.add_argument('--query-file', type=str,
                        help='Path to a file containing the SQL query to execute.')
    parser.add_argument('--output', type=str,
                        help='Output file to save results (e.g., results.csv).')
    parser.add_argument('--format', type=str, choices=[
                        'csv', 'xlsx', 'json'], default='csv', help='Output format for saving results.')
    parser.add_argument('--verbose', action='store_true',
                        help='Increase output verbosity.')
    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    # Read database configuration
    server, database, username, password, driver = read_config(args.config)

    # Get SQL query
    query = get_query(args)
    if not query.strip():
        logging.error("No SQL query provided.")
        sys.exit(1)

    # Connect to the database
    conn = create_connection(server, database, username, password, driver)

    # Execute the SQL query
    execute_query(conn, query, output_file=args.output,
                  output_format=args.format)

    # Close the database connection
    conn.close()


if __name__ == '__main__':
    main()
