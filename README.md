# dataframe_inserter

`dataframe_inserter` is a Python package for inserting Pandas DataFrames into MySQL, PostgreSQL, and Google Sheets with ease. The package provides classes that abstract the process of inserting data into different databases and Google Sheets, allowing you to work with data in a uniform manner across different storage solutions.

## Features
- **MySQL Integration**: Insert Pandas DataFrames into MySQL tables.
- **PostgreSQL Integration**: Insert Pandas DataFrames into PostgreSQL tables using the efficient `COPY` method.
- **Google Sheets Integration**: Insert Pandas DataFrames into Google Sheets.

## Installation

### From PyPI:
Once the package is published on PyPI, you will be able to install it via:

```bash
pip install dataframe_inserter
```

### From GitHub:
Alternatively, you can install the package directly from the GitHub repository:

```bash
pip install git+https://github.com/smasoudrezvani/dataframe_inserter.git
```

### Requirements:
The package relies on the following libraries:
- pandas
- sqlalchemy
- google-api-python-client
- google-auth
- psycopg2-binary
- pymysql

You can install the dependencies from the `requirements.txt` file:
```bash
pip install -r requirements.txt
```

## Usage

### 1. MySQL Test Example

```python
from dataframe_inserter import MySQLDatabaseInserter
import pandas as pd

# Sample DataFrame
df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

# Initialize the inserter with MySQL credentials
inserter = MySQLDatabaseInserter(db_user='root', db_password='password', db_host='localhost', db_port='3306', db_name='test_db')

# Insert the DataFrame into the specified table
inserter.insert_df(df, 'table_name')
```

### 2. PostgreSQL Test Example

```python
from dataframe_inserter import PostgreSQLDatabaseInserter
import pandas as pd

# Sample DataFrame
df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

# Initialize the inserter with PostgreSQL credentials
inserter = PostgreSQLDatabaseInserter(db_user='postgres', db_password='password', db_host='localhost', db_port='5432', db_name='test_db')

# Insert the DataFrame into the specified table
inserter.insert_df(df, 'table_name')
```

### 3. Google Sheets Test Example

```python
from dataframe_inserter import GoogleSheetsInserter
import pandas as pd

# Sample DataFrame
df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

# Initialize the Google Sheets inserter with a service account file
inserter = GoogleSheetsInserter(service_account_file='path_to_service_account.json')

# Insert the DataFrame into the specified Google Sheet
inserter.insert_df_to_google_sheet(df, 'spreadsheet_id', 'Sheet1', 'A', 'S')
```

## How It Works

### MySQL and PostgreSQL Inserters
For both MySQL and PostgreSQL, the `dataframe_inserter` uses `SQLAlchemy` to handle database interactions. The package provides different inserters (`MySQLDatabaseInserter` and `PostgreSQLDatabaseInserter`) that abstract away the complexity of interacting with these databases.

### Google Sheets Inserter
For Google Sheets, the package leverages Google Sheets API and Google OAuth credentials to interact with Google Sheets, allowing you to insert data into a spreadsheet using the `GoogleSheetsInserter`.

## Contributing

Feel free to contribute to this project by submitting issues or pull requests.

