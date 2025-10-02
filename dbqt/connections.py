from abc import ABC, abstractmethod
import os
import logging
from typing import Optional
import pyodbc
import jaydebeapi

import boto3
import time
from tqdm import tqdm
from sqeleton import table
from reladiff import connect as reladiff_connect

logger = logging.getLogger(__name__)


class DBConnector(ABC):
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.conn_type = config.get("type", "N/A")

    # @abstractmethod
    def connection_details(self):
        pass

    def connect(self):
        self.logger.info(f"Establishing connection to {self.conn_type}")
        self.connection = reladiff_connect(self.connection_details)
        self.logger.info(f"Connection established to {self.conn_type}")

    def disconnect(self):
        if self.connection:
            self.logger.info(f"Terminating connection to {self.conn_type}")
            self.connection.close()
            self.logger.info(f"Connection terminated for {self.conn_type}")

    def run_query(self, query):
        self.logger.info(f"Running {self.conn_type} query: {query[:300]}")
        result = self.connection.query(query)
        self.logger.info("Query completed successfully")
        return result

    def fetch_table_metadata(self, table_name):
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE upper(table_name) = upper('{table_name}')
        ORDER BY ordinal_position
        """
        self.logger.info(f"Fetching metadata for table: {table_name}")
        result = self.run_query(query)
        return [(row[0], row[1]) for row in result]

    def retrieve_table(self, table_name):
        return table(table_name)

    def fetch_all_columns(self, table_name):
        try:
            return {
                col[0].upper(): col[1] for col in self.fetch_table_metadata(table_name)
            }
        except Exception as e:
            print(f"Error retrieving source table metadata: {str(e)}")

    def count_rows(self, table_name, where_clause=None):
        """Retrieve the total number of rows in a table."""
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            result = self.run_query(query)
            return result[0][0]
        except Exception as e:
            self.logger.error(f"Error counting rows for {table_name}: {str(e)}")
            return None

    def generate_table_from_query(self, table_name, query):
        self.logger.info(f"Generating table {table_name}")
        self.run_query(f"DROP TABLE IF EXISTS {table_name}")
        self.run_query(f"CREATE TABLE {table_name} AS {query}")
        self.logger.info(f"Table {table_name} generated successfully")


class MySQL(DBConnector):
    @property
    def connection_details(self):
        conn_str = f"mysql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config.get('port', '3306')}/{self.config.get('database', 'information_schema')}"
        return conn_str

    def fetch_table_metadata(self, table_name):
        query = f"""
        SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_SCHEMA) = UPPER('{self.config['database']}') 
        AND UPPER(TABLE_NAME) = UPPER('{table_name}')
        ORDER BY ORDINAL_POSITION
        """
        self.logger.info(f"Fetching metadata for table: {table_name}")
        result = self.run_query(query)
        return [(row[0], row[1]) for row in result]


class Snowflake(DBConnector):
    @property
    def connection_details(self):
        auth = (
            "&authenticator=externalbrowser"
            if self.config.get("authenticator") == "externalbrowser"
            else ""
        )
        conn_str = f"snowflake://{self.config['user']}:{self.config.get('password', 'dummy')}@{self.config['account']}/{self.config['database']}/{self.config['schema']}?warehouse={self.config['warehouse']}&role={self.config['role']}{auth}"
        return conn_str

    def fetch_table_metadata(self, table_name):
        query = f"""
        SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE 
        UPPER(TABLE_CATALOG) = UPPER('{self.config['database']}') 
        AND UPPER(TABLE_SCHEMA) = UPPER('{self.config['schema']}') 
        AND UPPER(TABLE_NAME) = UPPER('{table_name}')
        ORDER BY ORDINAL_POSITION
        """
        self.logger.info(f"Fetching metadata for table: {table_name}")
        result = self.run_query(query)
        return [(row[0], row[1]) for row in result]


class DuckDB(DBConnector):
    def __init__(self, config):
        super().__init__(config)
        # Allow custom path from config, otherwise default to duckdb.db in current directory
        if config.get("database"):
            self.duck_db_path = config["database"]
        elif config.get("path"):
            self.duck_db_path = config["path"]
        else:
            self.duck_db_path = os.path.join(os.getcwd(), "duckdb.db")

    @property
    def connection_details(self):
        return self.duck_db_path

    def connect(self):
        import duckdb

        self.logger.info(f"Establishing connection to {self.conn_type}")
        self.connection = duckdb.connect(self.connection_details)
        self.logger.info(f"Connection established to {self.conn_type}")

    def retrieve_table(self, table_name):
        return table(table_name, self.connection)

    def run_query(self, query):
        self.logger.info(f"Running {self.conn_type} query: {query[:300]}")
        result = self.connection.query(query)
        self.logger.info("Query completed successfully")
        return result.fetchall() if result else "Success"


class CSV(DuckDB):
    def connect(self):
        super().connect()
        self.config["table"] = f"data{self.config.get('prefix', '')}"
        self.logger.info(f"Creating table from CSV source: {self.config['file']}")
        self.generate_table_from_query(
            self.config["table"],
            f"SELECT * FROM read_csv_auto('{self.config['file']}')",
        )


class Parquet(DuckDB):
    def connect(self):
        super().connect()
        self.config["table"] = f"data{self.config.get('prefix', '')}"
        self.logger.info(f"Creating table from Parquet source: {self.config['file']}")
        self.generate_table_from_query(
            self.config["table"], f"SELECT * FROM read_parquet('{self.config['file']}')"
        )


class S3Parquet(Parquet):
    def __init__(self, config):
        super().__init__(config)
        self.session = boto3.Session(
            profile_name=config.get("aws_profile", "default"),
            region_name=config.get("aws_region", "us-west-2"),
        )
        self.s3_client = self.session.client("s3")
        self.bucket = config["bucket"]
        self.key = config["key"]

    def fetch_parquet_from_s3(
        self, bucket: str, key: str, local_path: str
    ) -> Optional[str]:
        try:
            s3_object = self.s3_client.head_object(Bucket=bucket, Key=key)
            total_length = s3_object["ContentLength"]

            with tqdm(
                total=round(total_length / (1024 * 1024), 2),
                unit="MB",
                desc=f"Fetching {key}",
            ) as pbar:
                self.s3_client.download_file(
                    bucket,
                    key,
                    local_path,
                    Callback=lambda bytes_transferred: pbar.update(
                        round(bytes_transferred / (1024 * 1024), 2)
                    ),
                )
            return local_path

        except self.s3_client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                directory = "/".join(key.split("/")[:-1]) + "/"
                print(f"\nFile not found: s3://{bucket}/{key}")
                print(f"Listing objects in directory: s3://{bucket}/{directory}\n")

                paginator = self.s3_client.get_paginator("list_objects_v2")
                objects = []

                for page in paginator.paginate(Bucket=bucket, Prefix=directory):
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            if not obj["Key"].endswith("/"):
                                objects.append(obj["Key"])

                if not objects:
                    print("No objects found in this directory.")
                    raise FileNotFoundError(f"File not found: s3://{bucket}/{key}")

                reply = (
                    "Adjust path so only 1 file is in the path.\nAvailable objects:\n"
                )
                for i, obj in enumerate(objects, 1):
                    reply += f"{i}. {obj}\n"

                while True:
                    if len(objects) == 1:
                        selected_key = objects[0]
                        return self.fetch_parquet_from_s3(
                            bucket, selected_key, local_path
                        )
                    else:
                        raise FileNotFoundError(reply)
            raise

    def connect(self):
        local_path = os.path.join(os.getcwd(), "/tmp/temp.parquet")
        downloaded_path = self.fetch_parquet_from_s3(self.bucket, self.key, local_path)

        if downloaded_path:
            self.config["file"] = downloaded_path
            super().connect()
            os.remove(downloaded_path)
        else:
            raise Exception("Failed to fetch parquet file from S3")


class PostgreSQL(DBConnector):
    @property
    def connection_details(self):
        conn_str = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}:5432/{self.config['database']}"
        self.logger.info(
            f"PostgreSQL connection details: {conn_str.replace(self.config['password'], '****')}"
        )
        return conn_str


class SQLServer(DBConnector):
    @property
    def connection_details(self):
        # Build connection string for SQL Server / Azure Synapse
        driver = self.config.get("driver", "ODBC Driver 17 for SQL Server")

        conn_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={self.config['server']};DATABASE={self.config['database']};"
            f"UID={self.config['user']};PWD={self.config['password']};"
            f"Authentication=ActiveDirectoryPassword"
        )

        return conn_string

    def connect(self):
        self.logger.info(f"Establishing connection to {self.conn_type}")
        self.connection = pyodbc.connect(self.connection_details)

        self.logger.info(f"Connection established to {self.conn_type}")

    def run_query(self, query):
        self.logger.info(f"Running {self.conn_type} query: {query[:300]}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        self.logger.info("Query completed successfully")
        return result if result else "Success"

    def fetch_table_metadata(self, table_name):
        # Handle schema.table format
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            schema = self.config.get("schema", "dbo")
            table = table_name

        query = f"""
        SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_SCHEMA) = UPPER('{schema}') 
        AND UPPER(TABLE_NAME) = UPPER('{table}')
        ORDER BY ORDINAL_POSITION
        """
        self.logger.info(f"Fetching metadata for table: {schema}.{table}")
        result = self.run_query(query)
        return [(row[0], row[1]) for row in result]


class Oracle(DBConnector):
    def __init__(self, config):
        super().__init__(config)
        self.jdbc_driver = config.get("jdbc_driver", "oracle.jdbc.OracleDriver")
        self.jdbc_jar_path = config.get("jdbc_jar_path")
        if not self.jdbc_jar_path:
            raise ValueError("jdbc_jar_path is required for Oracle connection")

        # Set JAVA_HOME if not already set to help jaydebeapi find JVM
        if not os.environ.get("JAVA_HOME"):
            self._set_java_home()

    def _set_java_home(self):
        """Attempt to find and set JAVA_HOME if not already set."""
        possible_java_homes = [
            "/opt/homebrew/opt/openjdk",  # Homebrew on Apple Silicon
            "/usr/local/opt/openjdk",  # Homebrew on Intel Mac
            "/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home",  # Oracle Java
            "/usr/lib/jvm/default-java",  # Linux default
            "/usr/lib/jvm/java-11-openjdk-amd64",  # Common Linux path
        ]

        for java_home in possible_java_homes:
            if os.path.exists(java_home):
                os.environ["JAVA_HOME"] = java_home
                self.logger.info(f"Set JAVA_HOME to: {java_home}")
                return

        self.logger.warning(
            "Could not automatically detect JAVA_HOME. Please set it manually if connection fails."
        )

    @property
    def connection_details(self):
        # Build JDBC connection string
        host = self.config["host"]
        port = self.config.get("port", 1521)
        service_name = self.config.get("service_name")
        sid = self.config.get("sid")
        schema = self.config.get("schema", self.config["user"].upper())

        if service_name:
            jdbc_url = f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
        elif sid:
            jdbc_url = f"jdbc:oracle:thin:@{host}:{port}:{sid}"
        else:
            raise ValueError(
                "Either 'service_name' or 'sid' must be provided for Oracle connection"
            )

        return jdbc_url

    def connect(self):
        self.logger.info(f"Establishing connection to {self.conn_type}")
        jdbc_url = self.connection_details

        self.connection = jaydebeapi.connect(
            self.jdbc_driver,
            jdbc_url,
            [self.config["user"], self.config["password"]],
            self.jdbc_jar_path,
        )

        # Set the current schema if specified
        schema = self.config.get("schema", self.config["user"].upper())
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema}")
            cursor.close()
            self.logger.info(f"Set current schema to: {schema}")
        except Exception as e:
            self.logger.warning(f"Could not set current schema to {schema}: {str(e)}")

        self.logger.info(f"Connection established to {self.conn_type}")

    def disconnect(self):
        if self.connection:
            self.logger.info(f"Terminating connection to {self.conn_type}")
            self.connection.close()
            self.logger.info(f"Connection terminated for {self.conn_type}")

    def run_query(self, query):
        self.logger.info(f"Running {self.conn_type} query: {query[:300]}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        self.logger.info("Query completed successfully")
        return result if result else "Success"

    def fetch_table_metadata(self, table_name):
        # Handle schema.table format
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            schema = self.config.get("schema", self.config["user"].upper())
            table = table_name

        query = f"""
        SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME, DATA_TYPE
        FROM ALL_TAB_COLUMNS
        WHERE UPPER(OWNER) = UPPER('{schema}') 
        AND UPPER(TABLE_NAME) = UPPER('{table}')
        ORDER BY COLUMN_ID
        """
        self.logger.info(f"Fetching metadata for table: {schema}.{table}")
        result = self.run_query(query)
        return [(row[0], row[1]) for row in result]


class Athena(DBConnector):
    def __init__(self, config):
        super().__init__(config)
        self.session = boto3.Session(profile_name=config.get("aws_profile", "default"))
        self.aws_region = config.get("aws_region", "us-east-1")
        self.workgroup = config.get("workgroup", "primary")
        self.athena_client = self.session.client("athena", region_name=self.aws_region)

    def connect(self):
        self.logger.info("Athena connection established")

    def disconnect(self):
        self.logger.info("Athena connection terminated")

    def run_query(self, query):
        self.logger.info(f"Running Athena query: {query[:300]}")

        # Build QueryExecutionContext
        query_context = {"Catalog": self.config.get("catalog", "AwsDataCatalog")}

        # Only include Database if it's provided in config
        if self.config.get("database"):
            query_context["Database"] = self.config["database"]

        # Start query execution
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext=query_context,
            WorkGroup=self.workgroup,
        )

        query_execution_id = response["QueryExecutionId"]

        # Wait for query to complete with exponential backoff
        wait_time = 1  # Start with 1 second
        while True:
            query_status = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )["QueryExecution"]["Status"]

            state = query_status["State"]

            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                if state == "FAILED":
                    reason = query_status.get("StateChangeReason", "Unknown error")
                    raise Exception(f"Query failed: {reason}")
                elif state == "CANCELLED":
                    raise Exception("Query was cancelled")
                break

            # Exponential backoff with max of 10 seconds
            time.sleep(wait_time)
            wait_time = min(10, wait_time * 5)  # 1s -> 5s -> 10s -> 10s -> ...

        # Get query results
        results = []
        paginator = self.athena_client.get_paginator("get_query_results")

        try:
            for page in paginator.paginate(QueryExecutionId=query_execution_id):
                for row in page["ResultSet"]["Rows"]:
                    results.append([field.get("VarCharValue") for field in row["Data"]])
        except Exception as e:
            self.logger.error(f"Error fetching results: {str(e)}")
            raise

        return results

    def count_rows(self, table_name, where_clause=None):
        """Retrieve the total number of rows in a table."""
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            result = self.run_query(query)
            return result[1][0]
        except Exception as e:
            self.logger.error(f"Error counting rows for {table_name}: {str(e)}")
            return None


def create_connector(config):
    connector_map = {
        "MySQL": MySQL,
        "Snowflake": Snowflake,
        "CSV": CSV,
        "Parquet": Parquet,
        "S3Parquet": S3Parquet,
        "PostgreSQL": PostgreSQL,
        "DuckDB": DuckDB,
        "Athena": Athena,
        "SQLServer": SQLServer,
        "Oracle": Oracle,
    }
    connector_class = connector_map.get(config["type"])
    if not connector_class:
        logger.error(f"Unsupported connector type: {config['type']}")
        raise ValueError(f"Unsupported connector type: {config['type']}")
    logger.info(f"Initializing connector for type: {config['type']}")
    return connector_class(config)
