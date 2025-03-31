import logging
import os
import shutil
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from textwrap import shorten
from typing import List

import click
import oracledb
from codetiming import Timer
from dotenv import load_dotenv
import pyarrow
import pyarrow.parquet as pq

from . import __version__ as app_version

# Constants
TIMER_TEXT = "{name}: Elapsed time: {:.4f} seconds"

# Setup logging
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger()

# Load our environment file if it is present
load_dotenv(dotenv_path=".env")


class OracleParquetDumper:
    def __init__(self,
                 username: str,
                 password: str,
                 hostname: str,
                 service_name: str,
                 port: int,
                 schemas: List[str],
                 table_name_include_pattern: str,
                 table_name_exclude_pattern: str,
                 output_directory: str,
                 overwrite: bool,
                 compression_method: str,
                 batch_size: int,
                 row_limit: int,
                 isolation_level: str,
                 lowercase_object_names: bool,
                 logger: logging.Logger
                 ):
        self._username = username
        self._password = password
        self._dsn = oracledb.makedsn(host=hostname,
                                     port=port,
                                     service_name=service_name
                                     )

        self.schemas = schemas
        self.table_name_include_pattern = table_name_include_pattern
        self.table_name_exclude_pattern = table_name_exclude_pattern
        self.output_directory = output_directory
        self.overwrite = overwrite
        self.compression_method = compression_method
        self.batch_size = batch_size
        self.row_limit = row_limit
        self.isolation_level = isolation_level
        self.lowercase_object_names = lowercase_object_names
        self.logger = logger

        try:
            oracledb.init_oracle_client()
        except Exception as e:
            oracledb.init_oracle_client(lib_dir=os.getenv("ORACLE_LIB_DIR", os.getenv("ORACLE_HOME", "") + "/lib"))

    @contextmanager
    def get_db_connection(self) -> oracledb.Connection:
        con = oracledb.connect(user=self._username,
                               password=self._password,
                               dsn=self._dsn
                               )
        try:
            yield con
        finally:
            con.close()

    def get_columns(self,
                   connection: oracledb.Connection,
                   schema: str,
                   table_name: str
                   ):
        sql = f"""SELECT column_name
                    FROM all_tab_columns
                   WHERE owner = :schema
                     AND table_name = :table_name
                     AND data_type NOT IN ('BLOB', 'BFILE', 'CLOB', 'UNDEFINED', 'UROWID', 'LONG', 'RAW') /* These types are un-supported */
                     AND data_type NOT LIKE 'INTERVAL %'
                   ORDER BY column_id ASC
              """

        with connection.cursor() as cursor:
            cursor.execute(statement=sql,
                           schema=schema,
                           table_name=table_name
                           )
            column_list = [row[0] for row in cursor]

        return column_list

    def get_column_sql(self,
                       connection: oracledb.Connection,
                       schema: str,
                       table_name: str
                       ) -> str:
        column_list = self.get_columns(connection=connection,
                                       schema=schema,
                                       table_name=table_name
                                       )

        column_sql = ""
        for column_name in column_list:
            if self.lowercase_object_names:
                column_sql += f", \"{column_name}\" AS \"{column_name.lower()}\""
            else:
                column_sql += f", \"{column_name}\""

        return column_sql

    def dump_table(self,
                   connection: oracledb.Connection,
                   schema: str,
                   table_name: str,
                   output_path_prefix: str
                   ):
        column_sql = self.get_column_sql(connection=connection,
                                         schema=schema,
                                         table_name=table_name
                                         )

        if column_sql == "":
            self.logger.warning(f"Table: {schema}.{table_name} has no eligible dump columns, skipping.")
            return

        sql = f"SELECT {column_sql.strip(", ")} FROM \"{schema}\".\"{table_name}\""
        if self.row_limit != 0:
            sql += f" FETCH FIRST {self.row_limit} ROWS ONLY"

        self.logger.info(msg=f"Dumping table: {schema}.{table_name} - SQL: {sql}")

        pq_writer = None
        rows_dumped = 0

        Path(output_path_prefix).mkdir(parents=True, exist_ok=True)
        file_name = f"{output_path_prefix}/{table_name.lower() if self.lowercase_object_names else table_name}.parquet"
        for odf in connection.fetch_df_batches(statement=sql,
                                               size=self.batch_size
                                               ):
            # Get a PyArrow table from the query results
            pyarrow_table = pyarrow.Table.from_arrays(
                arrays=odf.column_arrays(), names=odf.column_names()
            )
            rows_dumped += pyarrow_table.num_rows

            if not pq_writer:
                pq_writer = pq.ParquetWriter(where=file_name,
                                            schema=pyarrow_table.schema,
                                            compression=self.compression_method
                                            )

            pq_writer.write_table(table=pyarrow_table)

        pq_writer.close()
        self.logger.info(f"Wrote {rows_dumped} rows to {file_name} - table: {schema}.{table_name}")

    def get_tables(self,
                   connection: oracledb.Connection,
                   schema: str
                   ):
        sql = f"""SELECT table_name
                    FROM all_tables
                   WHERE owner = :schema
                     AND external = 'NO'
                     AND temporary = 'N'
                     AND REGEXP_LIKE(table_name, :table_name_include_pattern)
                  ORDER BY table_name ASC
              """

        additional_bind_vars = {}
        if self.table_name_exclude_pattern:
            sql += f"AND NOT REGEXP_LIKE(table_name, :table_name_exclude_pattern)\n"
            additional_bind_vars["table_name_exclude_pattern"] = self.table_name_exclude_pattern

        with connection.cursor() as cursor:
            cursor.execute(statement=sql,
                           schema=schema,
                           table_name_include_pattern=self.table_name_include_pattern,
                           **additional_bind_vars
                           )
            object_list = [row[0] for row in cursor]

        return object_list

    def dump_tables(self):
        with self.get_db_connection() as connection:
            # Set the isolation level
            with connection.cursor() as cursor:
                cursor.execute(statement=f"ALTER SESSION SET ISOLATION_LEVEL = {self.isolation_level}")

            output_path = Path(self.output_directory)
            if output_path.exists():
                if self.overwrite:
                    shutil.rmtree(path=output_path.as_posix())
                    output_path.mkdir(parents=True)
                else:
                    raise RuntimeError(
                        f"Directory: {output_path.as_posix()} exists, aborting.")

            with Timer(name=f"Exporting tables - for schemas: {self.schemas}",
                       text=TIMER_TEXT,
                       initial_text=True,
                       logger=self.logger.info
                       ):
                for schema in self.schemas:
                    with Timer(name=f"Exporting objects - for schema: {schema}",
                               text=TIMER_TEXT,
                               initial_text=True,
                               logger=self.logger.info
                               ):
                        schema_output_path_prefix = Path(f"{self.output_directory}/{schema.lower() if self.lowercase_object_names else schema}")

                        table_list = self.get_tables(connection=connection,
                                                      schema=schema
                                                      )
                        for table_name in table_list:
                            table_output_path = Path(f"{schema_output_path_prefix}/{table_name.lower() if self.lowercase_object_names else table_name}")
                            with Timer(name=f"Dumping table: {schema}.{table_name} - to path: {table_output_path}",
                                       text=TIMER_TEXT,
                                       initial_text=True,
                                       logger=self.logger.info
                                       ):
                                self.dump_table(connection=connection,
                                                schema=schema,
                                                table_name=table_name,
                                                output_path_prefix=table_output_path
                                                )


@click.command()
@click.option(
    "--version/--no-version",
    type=bool,
    default=False,
    show_default=False,
    required=True,
    help="Prints the Oracle Parquet Dumper utility version and exits."
)
@click.option(
    "--username",
    type=str,
    default=os.getenv("DATABASE_USERNAME"),
    show_default=True,
    required=True,
    help="The Oracle database username to connect with."
)
@click.option(
    "--password",
    type=str,
    default=os.getenv("DATABASE_PASSWORD"),
    show_default=False,
    required=True,
    help="The Oracle database password to connect with."
)
@click.option(
    "--hostname",
    type=str,
    default=os.getenv("DATABASE_HOSTNAME"),
    show_default=True,
    required=True,
    help="The Oracle database hostname to connect to."
)
@click.option(
    "--service-name",
    type=str,
    default=os.getenv("DATABASE_SERVICE_NAME"),
    show_default=True,
    required=True,
    help="The Oracle database service name to connect to."
)
@click.option(
    "--port",
    type=int,
    default=os.getenv("DATABASE_PORT"),
    show_default=True,
    required=True,
    help="The Oracle database port to connect to."
)
@click.option(
    "--schema",
    type=str,
    default=[os.getenv("DATABASE_USERNAME", "").upper()],
    show_default=True,
    required=True,
    multiple=True,
    help="The schema to export objects for, may be specified more than once.  Defaults to the database username."
)
@click.option(
    "--table-name-include-pattern",
    type=str,
    default=".*",
    show_default=True,
    required=True,
    help="The regexp pattern to use to filter object names to include in the export."
)
@click.option(
    "--table-name-exclude-pattern",
    type=str,
    default=None,
    required=False,
    help="The regexp pattern to use to filter object names to exclude in the export."
)
@click.option(
    "--output-directory",
    type=str,
    default=Path("output").as_posix(),
    show_default=True,
    required=True,
    help="The path to the output directory - may be relative or absolute."
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Controls whether to overwrite any existing DDL export files in the output path."
)
@click.option(
    "--compression-method",
    type=click.Choice(["none", "snappy", "gzip", "zstd"]),
    default="zstd",
    show_default=True,
    required=True,
    help="The compression method to use for the parquet files generated."
)
@click.option(
    "--batch-size",
    type=int,
    default=int(os.getenv("BATCH_SIZE", 10_000)),
    show_default=True,
    required=True,
    help="The compression method to use for the parquet files generated."
)
@click.option(
    "--row-limit",
    type=int,
    default=int(os.getenv("ROW_LIMIT", "0")),
    show_default=True,
    required=True,
    help="The maximum number of rows to export from each table - useful for testing/debuggin purposes."
)
@click.option(
    "--isolation-level",
    type=str,
    default=os.getenv("ISOLATION_LEVEL", "SERIALIZABLE"),
    show_default=True,
    required=True,
    help="The Oracle session Isolation level - used to get a consistent export of table data with regards to System Change Number (SCN)."
)
@click.option(
    "--lowercase-object-names/--no-lowercase-object-names",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Controls whether the dump utility lower-cases the object names (i.e. schema, table, and column names)."
)
@click.option(
    "--log-level",
    type=str,
    default=os.getenv("LOGGING_LEVEL", "INFO"),
    show_default=True,
    required=True,
    help="The logging level to use for the application."
)
def main(version: bool,
         username: str,
         password: str,
         hostname: str,
         service_name: str,
         port: int,
         schema: List[str],
         table_name_include_pattern: str,
         table_name_exclude_pattern: str,
         output_directory: str,
         overwrite: bool,
         compression_method: str,
         batch_size: int,
         row_limit: int,
         isolation_level: str,
         lowercase_object_names: bool,
         log_level: str
         ):
    if version:
        print(f"Oracle Parquet Dumper - version: {app_version}")
        return

    logger.setLevel(level=getattr(logging, log_level))

    logger.info(msg=f"Starting Oracle Parquet Dumper application - version: {app_version}")
    arg_dict = locals()
    arg_dict.update({"password": "(redacted)"})
    logger.info(msg=f"Called with arguments: {arg_dict}")

    oracle_parquet_dumper = OracleParquetDumper(username=username,
                                                password=password,
                                                hostname=hostname,
                                                service_name=service_name,
                                                port=port,
                                                schemas=schema,
                                                table_name_include_pattern=table_name_include_pattern,
                                                table_name_exclude_pattern=table_name_exclude_pattern,
                                                output_directory=output_directory,
                                                overwrite=overwrite,
                                                compression_method=compression_method,
                                                batch_size=batch_size,
                                                row_limit=row_limit,
                                                isolation_level=isolation_level,
                                                lowercase_object_names=lowercase_object_names,
                                                logger=logger
                                                )

    oracle_parquet_dumper.dump_tables()


if __name__ == "__main__":
    main()
