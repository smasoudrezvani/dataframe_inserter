import pandas as pd
from .query_handler import PostgreSQLDatabaseHandler
from io import StringIO
import csv

class BaseDatabaseInserter:
    """
    Base class for handling DataFrame insertion into a database.
    """

    def __init__(self, db_user: str, db_password: str, db_host: str, db_port: str, db_name: str):
        """
        Initializes the database handler with connection parameters.

        Args:
            db_user (str): The username for the database.
            db_password (str): The password for the database.
            db_host (str): The host address of the database.
            db_port (str): The port number on which the database is running.
            db_name (str): The name of the database.
        """
        self.db_handler = self.create_db_handler(db_user, db_password, db_host, db_port, db_name)

    def create_db_handler(self, db_user: str, db_password: str, db_host: str, db_port: str, db_name: str):
        """
        Creates a database handler. This method should be implemented in subclasses to 
        handle different databases (e.g., PostgreSQL, MySQL).

        Args:
            db_user (str): The username for the database.
            db_password (str): The password for the database.
            db_host (str): The host address of the database.
            db_port (str): The port number on which the database is running.
            db_name (str): The name of the database.

        Returns:
            PostgreSQLDatabaseHandler: Database handler object for PostgreSQL.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def insert_df(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Inserts a Pandas DataFrame into the specified database table.

        Args:
            df (pd.DataFrame): The DataFrame to be inserted.
            table_name (str): The name of the target table in the database.

        Raises:
            Exception: If the DataFrame insertion fails due to database connectivity or query issues.
        """
        raise NotImplementedError("Subclasses must implement this method.")


class PostgreSQLDatabaseInserter(BaseDatabaseInserter):
    """
    Class for inserting a Pandas DataFrame into a PostgreSQL database.
    """

    def create_db_handler(self, db_user: str, db_password: str, db_host: str, db_port: str, db_name: str):
        """
        Creates a PostgreSQL database handler.

        Args:
            db_user (str): The username for the PostgreSQL database.
            db_password (str): The password for the PostgreSQL database.
            db_host (str): The host address of the PostgreSQL database.
            db_port (str): The port number on which the PostgreSQL database is running.
            db_name (str): The name of the PostgreSQL database.

        Returns:
            PostgreSQLDatabaseHandler: A PostgreSQL database handler object.
        """
        return PostgreSQLDatabaseHandler(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)

    def psql_insert_copy(self, table, conn, keys, data_iter) -> None:
        """
        Helper function to perform bulk insert into PostgreSQL using COPY.

        Args:
            table: SQLAlchemy Table object representing the target table.
            conn: SQLAlchemy Connection object.
            keys: List of column names to be inserted.
            data_iter: Iterator over the data to be inserted.

        Returns:
            None: This function writes data directly to the PostgreSQL database.
        """
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)
            columns = ', '.join(f'"{k}"' for k in keys)
            sql = f'COPY {table.name} ({columns}) FROM STDIN WITH CSV'
            cur.copy_expert(sql=sql, file=s_buf)

    def insert_df(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Inserts a Pandas DataFrame into a PostgreSQL database table.

        Args:
            df (pd.DataFrame): The DataFrame to be inserted.
            table_name (str): The name of the target table in the PostgreSQL database.

        Raises:
            Exception: If the DataFrame insertion fails due to database connectivity or query issues.
        """
        try:
            # Insert the DataFrame to PostgreSQL using the session handler and the COPY method
            df.to_sql(name=table_name, con=self.db_handler.engine, if_exists='append', index=False, method=self.psql_insert_copy)
            print(f"Data successfully inserted into {table_name}")
        except Exception as e:
            # Handle any exceptions that occur during the insertion process
            print(f"Failed to insert data: {e}")
        finally:
            # Close the PostgreSQL connection
            self.db_handler.close_connection()


# Usage Example (to be removed when packaging):
# inserter = PostgreSQLDatabaseInserter(db_user='postgres', db_password='password', db_host='localhost', db_port='5432', db_name='test_db')
# inserter.insert_df(df, 'table_name')
