import pandas as pd
from .query_handler import SQLDatabaseHandler


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
        handle different databases (e.g., MySQL, PostgreSQL).

        Args:
            db_user (str): The username for the database.
            db_password (str): The password for the database.
            db_host (str): The host address of the database.
            db_port (str): The port number on which the database is running.
            db_name (str): The name of the database.

        Returns:
            SQLDatabaseHandler: Database handler object.
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


class MySQLDatabaseInserter(BaseDatabaseInserter):
    """
    Class for inserting a Pandas DataFrame into a MySQL database.
    """

    def create_db_handler(self, db_user: str, db_password: str, db_host: str, db_port: str, db_name: str):
        """
        Creates a MySQL database handler.

        Args:
            db_user (str): The username for the MySQL database.
            db_password (str): The password for the MySQL database.
            db_host (str): The host address of the MySQL database.
            db_port (str): The port number on which the MySQL database is running.
            db_name (str): The name of the MySQL database.

        Returns:
            SQLDatabaseHandler: A MySQL database handler object.
        """
        return SQLDatabaseHandler(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)

    def insert_df(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Inserts a Pandas DataFrame into a MySQL database table.

        Args:
            df (pd.DataFrame): The DataFrame to be inserted.
            table_name (str): The name of the target table in the MySQL database.

        Raises:
            Exception: If the DataFrame insertion fails due to database connectivity or query issues.
        """
        try:
            # Insert the DataFrame into the specified MySQL table
            df.to_sql(name=table_name, con=self.db_handler.engine, if_exists='append', index=False)
            print(f"Data successfully inserted into {table_name}")
        except Exception as e:
            # Handle any exceptions that occur during the insertion process
            print(f"Failed to insert data: {e}")
        finally:
            # Close the MySQL connection
            self.db_handler.close_connection()


# Usage Example (to be removed when packaging):
# inserter = MySQLDatabaseInserter(db_user='root', db_password='password', db_host='localhost', db_port='3306', db_name='test_db')
# inserter.insert_df(df, 'table_name')
