import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
# from clickhouse_sqlalchemy import make_session
# import boto3
import warnings
warnings.filterwarnings("ignore")

class BaseDatabaseHandler:
    """
    Base class for handling database interactions. Subclasses should implement the 
    `build_url` and `setup` methods to establish connections to specific databases.
    """

    def __init__(self, **kwargs):
        """
        Initializes the BaseDatabaseHandler by setting up the engine and session.
        
        Args:
            kwargs: Connection parameters passed to the setup method of the subclass.
        """
        self.engine = None
        self.Session = None
        self.setup(**kwargs)

    def build_url(self, **kwargs) -> str:
        """
        Builds the database URL based on subclass-specific implementation.

        Args:
            kwargs: Connection parameters.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def setup(self, **kwargs) -> None:
        """
        Sets up the database engine and session.

        Args:
            kwargs: Connection parameters.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def execute_query(self, query: str) -> tuple:
        """
        Executes a SQL query and returns the results.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            tuple: A tuple containing the query results and column keys. 
                   Returns (None, None) if no rows are returned.

        Raises:
            Exception: If query execution fails.
        """
        session = self.Session()
        try:
            result = session.execute(text(query))
            session.commit()
            if result.returns_rows:
                return result.fetchall(), result.keys()
            else:
                return None, None
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def batch_update(self, update_sql: str, data: list[dict]) -> None:
        """
        Performs a batch update using the provided SQL and data.

        Args:
            update_sql (str): The SQL update statement.
            data (list[dict]): A list of dictionaries containing the update parameters.

        Raises:
            Exception: If batch update fails.
        """
        session = self.Session()
        try:
            for params in data:
                session.execute(text(update_sql), params)
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def close_connection(self) -> None:
        """
        Closes the connection to the database by disposing of the engine.
        """
        self.engine.dispose()

class SQLDatabaseHandler(BaseDatabaseHandler):
    """
    Handler for MySQL database connections and queries.
    """

    def build_url(self, **kwargs) -> str:
        """
        Builds the MySQL database connection URL.

        Args:
            kwargs: Connection parameters such as user, password, host, port, and database.

        Returns:
            str: The connection URL for the MySQL database.
        """
        user = kwargs.get('user')
        password = kwargs.get('password')
        host = kwargs.get('host')
        port = kwargs.get('port', '3306')
        database = kwargs.get('database', '')
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}" if database else f"mysql+pymysql://{user}:{password}@{host}:{port}"
    
    def setup(self, **kwargs) -> None:
        """
        Sets up the MySQL database engine and session.

        Args:
            kwargs: Connection parameters such as user, password, host, port, and database.
        """
        url = self.build_url(**kwargs)
        self.engine = create_engine(url)
        self.Session = sessionmaker(bind=self.engine)

class PostgreSQLDatabaseHandler(BaseDatabaseHandler):
    """
    Handler for PostgreSQL database connections and queries.
    """

    def build_url(self, **kwargs) -> str:
        """
        Builds the PostgreSQL database connection URL.

        Args:
            kwargs: Connection parameters such as user, password, host, port, and database.

        Returns:
            str: The connection URL for the PostgreSQL database.
        """
        user = kwargs.get('user')
        password = kwargs.get('password')
        host = kwargs.get('host')
        port = kwargs.get('port', '5432')
        database = kwargs.get('database', '')
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}" if database else f"postgresql+psycopg2://{user}:{password}@{host}:{port}"

    def setup(self, **kwargs) -> None:
        """
        Sets up the PostgreSQL database engine and session.

        Args:
            kwargs: Connection parameters such as user, password, host, port, and database.
        """
        url = self.build_url(**kwargs)
        self.engine = create_engine(url)
        self.Session = sessionmaker(bind=self.engine)

class ClickhouseDatabaseHandler(BaseDatabaseHandler):
    """
    Handler for ClickHouse database connections and queries.
    """

    def build_url(self, **kwargs) -> str:
        """
        Builds the ClickHouse database connection URL.

        Args:
            kwargs: Connection parameters such as user, password, host, and database.

        Returns:
            str: The connection URL for the ClickHouse database.
        """
        user = kwargs.get('user')
        password = kwargs.get('password')
        host = kwargs.get('host')
        database = kwargs.get('database', '')
        return f"clickhouse://{user}:{password}@{host}/{database}" if database else f"clickhouse://{user}:{password}@{host}"

    def setup(self, **kwargs) -> None:
        """
        Sets up the ClickHouse database engine.

        Args:
            kwargs: Connection parameters such as user, password, host, and database.
        """
        url = self.build_url(**kwargs)
        print(url)
        self.engine = create_engine(url)

    def execute_query(self, query: str) -> tuple:
        """
        Executes a SQL query in ClickHouse and returns the results.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            tuple: A tuple containing the query results and column keys. 
                   Returns (None, None) if no rows are returned.

        Raises:
            Exception: If query execution fails.
        """
        session = make_session(self.engine)
        try:
            result = session.execute(text(query))
            session.commit()
            if result.returns_rows:
                return result.fetchall(), result.keys()
            else:
                return None, None
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def batch_update(self, update_sql: str, data: list[dict]) -> None:
        """
        Performs a batch update in ClickHouse using the provided SQL and data.

        Args:
            update_sql (str): The SQL update statement.
            data (list[dict]): A list of dictionaries containing the update parameters.

        Raises:
            Exception: If batch update fails.
        """
        session = make_session(self.engine)
        try:
            for params in data:
                session.execute(text(update_sql), params)
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
