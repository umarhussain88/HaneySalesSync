import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, types, text
from sqlalchemy.engine import URL


@dataclass
class AzureExporter:
    sql_server: str = None
    sql_user: str = None
    sql_password: str = None
    sql_db: str = None

    def __post_init__(self):
        self.cxn = URL.create(
            drivername="mssql+pyodbc",
            username=self.sql_user,
            password=self.sql_password,
            host=self.sql_server,
            database=self.sql_db,
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "Encrypt": "yes",
                "TrustServerCertificate": "yes",
                "Connection Timeout": "30",
            },
        )

        self.engine = create_engine(self.cxn, fast_executemany=True)

    def check_if_schema_exists(self, schema: str) -> bool:
        """Checks if schema exists in database.
        if it does not exist it will be created.

        Args:
            schema (str): schema name

        Returns:
            bool: True if schema exists, False if not.
        """

        sql_str = f"""
          IF schema_id ('{schema}') IS NULL
                BEGIN
                EXEC ('CREATE SCHEMA {schema} AUTHORIZATION dbo;')
                END
        """
        return self.engine.execute(sql_str)

    def insert_to_raw(
        self, dataset: pd.DataFrame, table_name: str, schema: Optional[str] = "stg"
    ) -> None:
        """inserts dataset into a raw table.
           As this is a raw table, it will be replaced if it already exists.

        Args:
            dataset (pd.DataFrame): Pandas dataframe that holds raw API data.
            table_name (str): target table name
            schema (Optional[str], optional): target schema in database. Defaults to 'raw'.
        Returns:
            None
        """

        dataset["_inserted_at"] = pd.to_datetime("now").utcnow()

        # create sqlalchemy types for each column
        col_types = {
            col: types.VARCHAR(dataset[col].astype(str).str.len().max())
            for col in dataset.columns
            if col not in ["_inserted_at"]
        }

        col_types["_inserted_at"] = types.DateTime()

        self.check_if_schema_exists(schema)

        dataset.to_sql(
            name=table_name,
            schema=schema,
            con=self.engine,
            if_exists="replace",
            index=False,
            dtype=col_types,
        )

        return None

    def scale_database(self, scale_sku: str):
        if (
            pd.read_sql("SELECT @@SERVERNAME as servername", self.engine)["servername"][
                0
            ]
            == "data-metrics-server"
        ):
            self.engine.execute(
                f"ALTER DATABASE {self.sql_db} MODIFY (EDITION = 'Standard', SERVICE_OBJECTIVE = '{scale_sku}')"
            )
        else:
            pass

    def execute_stored_procedure(self, sp_name: str, schema: Optional[str] = "stg"):
        with self.engine.begin() as conn:
            conn.execute(f"EXEC {schema}.{sp_name}")


@dataclass
class PostgresExporter:
    username: str = None
    password: str = None
    host: str = None
    port: str = None
    database: str = None

    def __post_init__(self):
        self.connection_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.engine = create_engine(self.connection_string)

    def check_if_schema_exists(self, schema):
        with self.engine.connect() as connection:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    def _clean_column_names(self, dataset: pd.DataFrame) -> pd.DataFrame:
        dataset.columns = (
            dataset.columns.str.strip()
            .str.lower()
            .str.replace("\s|-|\|/|\.|\(|\)", "_", regex=True)
        )
        # remove doulbe underscores and trailing underscores
        dataset.columns = dataset.columns.str.replace("__", "_", regex=True).str.rstrip(
            "_"
        )
        return dataset

    def insert_raw_data(
        self,
        dataset: pd.DataFrame,
        table_name: str,
        schema: str,
        created_at_column: Optional[str] = "created_at",
    ) -> None:
        
        if dataset.empty:
            pass

        else:
        
            dataset = self._clean_column_names(dataset)

            dataset = dataset.astype(str)
            dataset[created_at_column] = pd.to_datetime("now").utcnow()

            col_types = {
                col: types.VARCHAR(dataset[col].astype(str).str.len().max())
                for col in dataset.columns
                if col not in [created_at_column]
            }

            col_types[created_at_column] = types.DateTime()
            dataset = dataset.replace('nan', None)

            dataset.to_sql(
                name=table_name,
                schema=schema,
                con=self.engine,
                if_exists="append",
                index=False,
                dtype=col_types,
            )

    def check_if_record_exists(
        self,
        table_name: str,
        schema: str,
        source_dataframe: pd.DataFrame,
        look_up_column: str,
    ) -> pd.DataFrame:
        """
        Checks to see if we've already imported the file.
        
        returns a dataframe of the records that do not exist in the database.
        (Left anti join on id)
        """
        
        look_up_vals = source_dataframe[look_up_column].unique().tolist()

        query = f"""
        
        SELECT id FROM {schema}.{table_name} WHERE {look_up_column} IN  ({', '.join(f"'{item}'" for item in look_up_vals)})
        
        """

        current_files = pd.read_sql(query, self.engine)

        return source_dataframe.loc[
            ~source_dataframe[look_up_column].isin(current_files[look_up_column])
        ]

    def get_uuid_from_table(
        self, table_name: str, schema: str, look_up_val: str, look_up_column
    ) -> pd.DataFrame:
        with self.engine.connect() as connection:
            query = f"""
            SELECT uuid FROM {schema}.{table_name} WHERE {look_up_column} = '{look_up_val}'
            """
            return pd.read_sql(query, connection)
