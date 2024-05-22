"""
This driver module is part of an ETL project (extract, transform, load).
It's meant to be imported by main.py script and used to load dataframes to a MySQL database

v.2023-12-26 - Initial commit
"""

import pgdb
import pandas as pd
from datetime import datetime

try:
    from src._drv_hashicorp_vault import HashiVaultClient
except Exception:
    from _drv_hashicorp_vault import HashiVaultClient


def test():

    # Sample dataframe to test Class and Methods
    sample_dict = {
        "date_col": pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"]),
        "datetime_col": pd.to_datetime(["2022-01-01 00:00:00", "2022-01-01 01:00:00", "2022-01-01 02:00:00",
                                        "2022-01-01 03:00:00", "2022-01-01 04:00:00"]),
        "int_col": [8, 3, 1, 4, 5],
        "float_col": [0.5206506, 0.24434467, 0.09430657, 0.40720872, 0.66723723],
        "varchar_col": ["Text 1", "Text 2", "Text 3", "Text 4", "Text 5"],
        "varchar_col_2": ["Text 6", "Text 7", "Text 8", "Text 9", "Text 10"],

        "varchar_col_3": ["😊", "🌟", "🐻", "🍕", "⚡"]
    }
    sample_df = pd.DataFrame(sample_dict)

    # Test module using sample dataframe
    secret_path = "engenharia@cluster-postgresql-0001-01"
    database_name = "HC_FUTUREBRAND"  # "HC_FUTUREBRAND"
    table_name = "olap__test_table"
    column_name = "id"

    handler = PSQLHandler(secret_path, database_name)
    handler.connect()

    tb_true = handler.check_table_exists(table_name)
    if not tb_true:
        handler.create_table_from_df(table_name, sample_df)
    handler.check_column_exists(table_name, column_name)
    handler.get_unique_constraint_columns(table_name)
    handler.append_df_bulk(table_name, sample_df)

    sql_query = "DELETE FROM olap__test_table WHERE date_col::text LIKE '2022-01-01%'"
    response = handler.execute_sql_query(sql_query)
    if response is not None:
        print("\nINFO  - Query result:")
        for row in response:
            print(row)

    handler.delete_table_contents(table_name)
    handler.drop_table(table_name)
    handler.connection.close()


class PSQLHandler:
    """
    Class to handle PostgreSQL connection and CRUD operations.
    """

    def __init__(self, secret_path: str, database_name: str):
        """
        Initializes the PostgreSQLHandler with database credentials obtained from HashiCorp Vault.

        :param database_name: The name of the database to connect to.
        """
        status, secret = HashiVaultClient().get_secret(secret_path)
        if status is True:
            self.host = secret["host"]
            self.port = secret["port"]
            self.user = secret["user"]
            self.password = secret["password"]
            self.database = database_name
            self.conn_status = False

        else:
            print(f"\nERROR  - Invalid Hashi-Vault API request for secret '{secret_path}'")
            raise Exception

    def connect(self) -> bool:
        """
        Connects to the PostgreSQL database.

        Returns:
        - True if connection is successful, False otherwise.
        """
        try:
            self.connection = pgdb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )

            print(f"INFO  - Connected to database {self.database}")
            return True

        except Exception as err:
            print("\nERROR  - Failure connecting to PostgreSQL", err)
            return False

    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the connected PostgreSQL database.

        Parameters:
        - table_name (str): The name of the table to check.

        Returns:
        - True if the table exists, False otherwise.
        """
        try:
            print(f"INFO  - Checking if table '{table_name}' exists")
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');")
                result = cursor.fetchone()

                if result[0] is True:
                    print(f"INFO  - The table '{table_name}' already EXISTS.")
                else:
                    print("INFO  - The table does NOT EXIST.")

                return result[0]

        except pgdb.Error as err:
            print("ERROR  - Failure checking if table exists:", err)
            return False

    def check_column_exists(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column exists in the specified table of the connected PostgreSQL database.

        Parameters:
        - table_name (str): The name of the table to check.
        - column_name (str): The name of the column to check.

        Returns:
        - True if the column exists, False otherwise.
        """
        try:
            print(f"INFO  - Checking if column '{column_name}' exists on table '{table_name}'")
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
                )
                column_list = [row[0] for row in cursor.fetchall()]
                result = True if column_name in column_list else False

                if result:
                    print(f"INFO  - The column '{column_name}' EXISTS on table.")
                else:
                    print("INFO  - The column does NOT EXIST.")

                return result

        except pgdb.Error as err:
            print("\nERROR  - Failure checking if column exists", err)
            return False

    def get_sql_type_from_dtype(self, dtype) -> str:
        """
        Map pandas data types to PostgreSQL data types
        """
        df_to_postgresql_dtype = {
            "int64": "INT",
            "float64": "FLOAT",
            "datetime64[ns]": "TIMESTAMP",
            "object": "TEXT",
        }
        return df_to_postgresql_dtype.get(str(dtype), "TEXT")

    def get_unique_constraint_columns(self, table_name: str) -> list:
        """
        Get the columns forming a unique constraint on the specified table.

        Parameters:
        - table_name: The name of the table to check.

        Returns:
        - List of column names forming a unique constraint, or an empty list if no unique constraint is found.
        """
        unique_columns = []

        try:
            with self.connection.cursor() as cursor:
                # Query to retrieve unique constraints
                query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND column_name IN (
                    SELECT column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
                    WHERE constraint_type = 'UNIQUE'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
                    )
                """
                cursor.execute(query, (table_name, 'public', table_name))
                unique_columns = [row[0] for row in cursor.fetchall()]

                if len(unique_columns) == 0:
                    print("INFO  - No constraint for unique values / columns were found.")
                    return None

                if len(unique_columns) != 0:
                    print(f"INFO  - Found {len(unique_columns)} unique columns: {unique_columns}")
                    return unique_columns

        except pgdb.Error as err:
            print(f"ERROR - Failure retrieving unique constraint columns for table {table_name}: {err}")

    def create_table_from_df(self, table_name: str, dataframe: str) -> bool:
        """
        Creates a table in the database based on the columns of a DataFrame.

        Parameters:
        - table_name (str): The name of the table to create.
        - dataframe (Dataframe): The DataFrame whose columns will define the table.

        Returns:
        - True if table creation is successful, False otherwise.
        """

        try:
            print(f"INFO  - Creating table '{table_name}'")
            with self.connection.cursor() as cursor:
                # Add index column
                columns = ["id SERIAL PRIMARY KEY"]

                # Add dataframe columns headers and types
                for column, dtype in dataframe.dtypes.items():
                    if "datetime" not in column and "date" in column:
                        columns.append(f"{column} DATE")
                    else:
                        columns.append(f"{column} {self.get_sql_type_from_dtype(dtype)}")

                # Add the log column for datetime values
                columns.append("insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                create_table_query = ", ".join(columns) + ")"

                # Construct the CREATE TABLE query
                create_table_query = f"CREATE TABLE IF NOT EXISTS public.{table_name} ({', '.join(columns)})"
                cursor.execute(create_table_query)
                self.connection.commit()
                print("INFO  - Success creating table.")
                return True

        except pgdb.Error as err:
            print("\nERROR  - Failure creating table", err)
            return False

    def append_df_bulk(self, table_name: str, dataframe: pd.DataFrame,
                       constraint_column: str = None, batch_size: int = 10000):
        """
        Appends a DataFrame to a table in the database in bulk.

        Parameters:
        - table_name: The name of the table to append data to.
        - dataframe: The DataFrame containing the data to append.
        - batch_size: The size of each batch for bulk insertion.

        Returns:
        - True if the operation is successful, False otherwise.
        """
        dataframe.name = dataframe.name if hasattr(dataframe, "name") else "dataframe object"
        print(f"INFO  - Appending {dataframe.name} to table '{table_name}' in bulk")

        try:
            self.connection.execute("SAVEPOINT before_bulk_insert")
            cursor = self.connection.cursor()

            # Check dataframe for list columns and parse only the first value
            for column in dataframe.columns:
                if dataframe[column].apply(lambda x: isinstance(x, list)).any():
                    print(f"INFO  - Found instance LIST in dataframe column '{column}'")
                    print("INFO  - Parsing list first value and discarding others")
                    dataframe[column] = dataframe[column].apply(
                        lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)

            # Get column names and data types
            columns = dataframe.columns.tolist()
            data_types = dataframe.dtypes.tolist()

            columns.append("insert_time")  # Add the log column to the list of columns

            placeholders = ", ".join(["%s"] * len(columns))  # Generate the parameter placeholders
            data = []  # Create the data list for bulk insertion

            for _, row in dataframe.iterrows():
                row_data = []
                for i, value in enumerate(row.values):
                    if pd.isna(value):
                        row_data.append(None)  # Replace NaN with None in SQL
                    else:
                        if data_types[i] == "datetime64[ns]":  # Convert timestamp column to MySQL recognized format
                            value = value.strftime("%Y-%m-%d")
                        row_data.append(value)
                        # row_data.append(value.strftime("%Y-%m-%d") if data_types[i] == "datetime64[ns]" else value)

                data.append(row_data)

            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            if constraint_column:
                # Modify the INSERT query to handle conflicts and update values
                insert_query += f" ON CONFLICT ({constraint_column}) DO UPDATE SET " + \
                                ", ".join(f"{col}=EXCLUDED.{col}" for col in columns if col is not constraint_column)

            # Split the data into smaller batches
            batched_data = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]

            timestamp = datetime.now()
            total_inserted_rows = 0
            total_updated_rows = 0

            for i, batch in enumerate(batched_data):
                # print("Executing SQL query:", insert_query)  # DEBUG print the query before execution
                cursor.executemany(insert_query, [row + [timestamp] for row in batch])
                self.connection.commit()
                updated_rows = cursor.rowcount - len(batch)
                inserted_rows = len(batch) - updated_rows
                total_inserted_rows += inserted_rows
                total_updated_rows += updated_rows

                print(f"INFO  - ... Batch {i+1}/{len(batched_data)} inserted.")

            print("INFO  - Success appending dataframe to table.",
                  f"INFO  - ... {total_inserted_rows} inserted; {total_updated_rows} updated rows", sep="\n")
            return True

        except pgdb.Error as err:
            print("\nERROR  - Failure appending dataframe to table", err)
            # print("SQL Query:", insert_query)
            # print("Batch:", batch)  # DEBUG
            self.connection.execute("ROLLBACK TO SAVEPOINT before_bulk_insert")
            return False

        finally:
            if cursor:
                cursor.close()

    def delete_table_contents(self, table_name: str) -> bool:
        """
        Deletes all contents from a table in the database.

        Parameters:
        - table_name: The name of the table whose contents should be deleted.

        Returns:
        True if the operation is successful, False otherwise.
        """
        try:
            print(f"INFO  - Deleting contents from table '{table_name}'")
            with self.connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM {table_name}")
                self.connection.commit()

                print("INFO  - Success deleting table contents.")
                return True

        except pgdb.Error as err:
            print("\nERROR  - Failure deleting table contents", err)
            return False

    def drop_table(self, table_name: str) -> bool:
        """
        Deletes / drops a table from the database.

        Parameters:
        - table_name (str): The name of the table to drop.

        Returns:
        True if table deletion is successful, False otherwise.
        """
        try:
            print(f"INFO  - Dropping table '{table_name}'")
            with self.connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.connection.commit()

                print(f"INFO  - Success dropping table '{table_name}'.")
                return True

        except pgdb.Error as err:
            print("\nERROR  - Failure dropping table:", err)
            return False

    def execute_sql_query(self, sql_query: str):
        """
        Executes a full SQL query in the database.

        :param sql_query: The SQL query to be executed.

        :return: For SELECT queries returns a list of tuples representing the result set.
                 For non-SELECT queries returns PostgreSQL notices.
                 Returns None if the operation fails.
        """
        try:
            print(f"INFO  - Executing SQL query: {sql_query}")
            self.connection.execute("SAVEPOINT before_query")
            sql_query = " ".join(sql_query.split())

            with self.connection.cursor() as cursor:
                cursor.execute(sql_query)

                if sql_query.strip().upper().startswith("SELECT"):
                    # For SELECT queries, fetch the results
                    result_set = cursor.fetchall()
                    print("INFO  - Success executing SQL SELECT query.")
                    return result_set

                else:
                    # For other types of queries, return the response
                    rows_affected = cursor.rowcount
                    print(f"INFO  - ... {rows_affected} rows affected (Success).")
                    self.connection.commit()
                    return None

        except pgdb.Error as err:
            print(f"\nERROR  - Failure executing SQL query: {err}")
            self.connection.execute("ROLLBACK TO SAVEPOINT before_query")
            return None


if __name__ == "__main__":
    test()

# ------------------------------------------------------------------------------------------
# END of module
# ------------------------------------------------------------------------------------------
