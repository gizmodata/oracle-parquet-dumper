from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from conftest import ORACLE_PWD, ORACLE_PORT, os
from oracle_parquet_dumper.main import dumper


def test_basics(oracle_server):
    with TemporaryDirectory() as tmpdir:
        dumper(version=False,
               username="SYSTEM",
               password=ORACLE_PWD,
               hostname="localhost",
               service_name="FREEPDB1",
               port=ORACLE_PORT,
               schema=["SYSTEM"],
               table_name_include_pattern="HELP",
               table_name_exclude_pattern=None,
               output_directory=tmpdir,
               overwrite=True,
               compression_method="zstd",
               batch_size=10_000,
               row_limit=-1,
               isolation_level="SERIALIZABLE",
               lowercase_object_names=False,
               log_level="INFO"
               )

        # Check if the parquet file is created
        parquet_file_path = os.path.join(tmpdir, "SYSTEM", "HELP", "HELP.parquet")

        # Read the parquet file and print out a dataframe
        table = pq.read_table(parquet_file_path)

        assert table.num_rows > 0
        print(table.to_pandas())
