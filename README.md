# Oracle Parquet Dumper

[<img src="https://img.shields.io/badge/GitHub-gizmodata%2Foracle--parquet--dumper-blue.svg?logo=Github">](https://github.com/gizmodata/oracle-parquet-dumper)
[![oracle-parquet-dumper-ci](https://github.com/gizmodata/oracle-parquet-dumper/actions/workflows/ci.yml/badge.svg)](https://github.com/gizmodata/oracle-parquet-dumper/actions/workflows/ci.yml)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/sidewinder-db)](https://pypi.org/project/sidewinder-db/)
[![PyPI version](https://badge.fury.io/py/oracle-parquet-dumper.svg)](https://badge.fury.io/py/oracle-parquet-dumper)
[![PyPI Downloads](https://img.shields.io/pypi/dm/oracle-parquet-dumper.svg)](https://pypi.org/project/oracle-parquet-dumper/)

The Oracle Parquet Dumper utility is a command-line tool that allows you to export Oracle database objects (tables, views, etc.) to Parquet files. It can be used in conjunction with the [GizmoSQL](https://gizmodata.com/gizmosql) database engine to hyper-accelerate Oracle SQL workloads.

## Install package
You can install `oracle-parquet-dumper` from source.

### Option 1 - from PyPi
```shell
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

pip install oracle-parquet-dumper
```

### Option 2 - from source - for development
```shell
git clone https://github.com/gizmodata/oracle-parquet-dumper.git

cd oracle-parquet-dumper

# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install Sidewinder-DB - in editable mode with dev dependencies
pip install --editable .[dev]
```

### Note
For the following commands - if you running from source and using `--editable` mode (for development purposes) - you will need to set the PYTHONPATH environment variable as follows:
```shell
export PYTHONPATH=$(pwd)/src
```

## Usage
### Help
```shell
oracle-parquet-dumper --help
Usage: oracle-parquet-dumper [OPTIONS]

Options:
  --version / --no-version        Prints the Oracle Parquet Dumper utility
                                  version and exits.  [required]
  --username TEXT                 The Oracle database username to connect
                                  with.  [required]
  --password TEXT                 The Oracle database password to connect
                                  with.  [required]
  --hostname TEXT                 The Oracle database hostname to connect to.
                                  [required]
  --service-name TEXT             The Oracle database service name to connect
                                  to.  [required]
  --port INTEGER                  The Oracle database port to connect to.
                                  [default: 1521; required]
  --schema TEXT                   The schema to export objects for, may be
                                  specified more than once.  Defaults to the
                                  database username.  [required]
  --table-name-include-pattern TEXT
                                  The regexp pattern to use to filter object
                                  names to include in the export.  [default:
                                  .*; required]
  --table-name-exclude-pattern TEXT
                                  The regexp pattern to use to filter object
                                  names to exclude in the export.
  --output-directory TEXT         The path to the output directory - may be
                                  relative or absolute.  [default: output;
                                  required]
  --overwrite / --no-overwrite    Controls whether to overwrite any existing
                                  DDL export files in the output path.
                                  [default: no-overwrite; required]
  --compression-method [none|snappy|gzip|zstd]
                                  The compression method to use for the
                                  parquet files generated.  [default: zstd;
                                  required]
  --batch-size INTEGER            The compression method to use for the
                                  parquet files generated.  [default: 10000;
                                  required]
  --row-limit INTEGER             The maximum number of rows to export from
                                  each table - useful for testing/debuggin
                                  purposes.  [default: 0; required]
  --isolation-level TEXT          The Oracle session Isolation level - used to
                                  get a consistent export of table data with
                                  regards to System Change Number (SCN).
                                  [default: SERIALIZABLE; required]
  --lowercase-object-names / --no-lowercase-object-names
                                  Controls whether the dump utility lower-
                                  cases the object names (i.e. schema, table,
                                  and column names).  [default: no-lowercase-
                                  object-names; required]
  --log-level TEXT                The logging level to use for the
                                  application.  [default: INFO; required]
  --help                          Show this message and exit.
```

## Handy development commands

#### Version management

##### Bump the version of the application - (you must have installed from source with the [dev] extras)
```bash
bumpver update --patch
```
