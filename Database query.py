import pymssql
import pandas as pd
import json



def main():
    print('Python HTTP trigger function processed a request.')

    # Extract the SQL query and server parameters from the request body
    try:
        with open("info.json", "r") as file:
            data = json.load(file)
            
        query = "SELECT * FROM YourTable"
        server = data.get('server')
        database = data.get('database')
        username = data.get('username')
        password = data.get('password')
    except Exception as e:
        print(f'Error: {e}')

    if not all([server, database, username, password, query]):
        print("Missing one or more environment variables (SERVER, DATABASE, USERNAME, PASSWORD, SQLQUERY).")
        return 

    try:
        # Connect to the Azure SQL Database
        conn = pymssql.connect(server=server, user=username,
                               password=password, database=database)
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Check if the query is an INSERT, UPDATE, or DELETE
        if query.strip().lower().startswith(('insert', 'update', 'delete')):
            # Commit the changes for data-modifying queries
            conn.commit()
            print(f"Query executed successfully: {query}")
            return 

        # If it's a SELECT query, fetch the result set
        rows = cursor.fetchall()

        # Create a DataFrame from the query results
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])

        # Check if the DataFrame is empty
        if df.empty:
            print("No data found")
            return 

        # Convert the DataFrame to a string for returning in the HTTP response
        result_str = df.to_string(index=False)

        print(f"Query result: \n\n{result_str}")
        return 

    except Exception as e:
        print(f"Error connecting to database: {str(e)}\nQuery: {query}")
        return 

    finally:
        # Ensure the connection is closed
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    main()