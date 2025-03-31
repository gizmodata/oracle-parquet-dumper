# Oracle Object Tracker

[<img src="https://img.shields.io/badge/GitHub-TruckCab%2Foracle--object--tracker-blue.svg?logo=Github">](https://github.com/TruckCab/oracle-object-tracker)
[![oracle-object-tracker-ci](https://github.com/TruckCab/oracle-object-tracker/actions/workflows/ci.yml/badge.svg)](https://github.com/TruckCab/oracle-object-tracker/actions/workflows/ci.yml)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/sidewinder-db)](https://pypi.org/project/sidewinder-db/)
[![PyPI version](https://badge.fury.io/py/oracle-object-tracker.svg)](https://badge.fury.io/py/oracle-object-tracker)
[![PyPI Downloads](https://img.shields.io/pypi/dm/oracle-object-tracker.svg)](https://pypi.org/project/oracle-object-tracker/)

## Install package
You can install `oracle-object-tracker` from source.

### Option 1 - from PyPi
```shell
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

pip install oracle-object-tracker
```

### Option 2 - from source - for development
```shell
git clone https://github.com/TruckCab/oracle-object-tracker.git

cd oracle-object-tracker

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
oracle-object-tracker --help
Usage: oracle-object-tracker [OPTIONS]

Options:
  --version / --no-version        Prints the Oracle Object Tracker version and
                                  exits.  [required]
  --username TEXT                 The Oracle database username to connect
                                  with.  [required]
  --password TEXT                 The Oracle database password to connect
                                  with.  [required]
  --hostname TEXT                 The Oracle database hostname to connect to.
                                  [required]
  --service-name TEXT             The Oracle database service name to connect
                                  to.  [required]
  --port INTEGER                  The Oracle database port to connect to.
                                  [required]
  --schema TEXT                   The schema to export objects for, may be
                                  specified more than once.  Defaults to the
                                  database username.  [required]
  --object-type TEXT              The object types to export.  [default:
                                  CLUSTER, DATABASE LINK, FUNCTION, INDEX,
                                  JAVA SOURCE, JOB, MATERIALIZED VIEW,
                                  MATERIALIZED VIEW LOG, PACKAGE, PACKAGE
                                  BODY, PROCEDURE, SEQUENCE, SYNONYM, TABLE,
                                  TRIGGER, TYPE, TYPE BODY, VIEW; required]
  --object-name-include-pattern TEXT
                                  The regexp pattern to use to filter object
                                  names to include in the export.  [default:
                                  .*; required]
  --object-name-exclude-pattern TEXT
                                  The regexp pattern to use to filter object
                                  names to exclude in the export.
  --output-directory TEXT         The path to the output directory - may be
                                  relative or absolute.  [default: /var/folder
                                  s/c8/b5pj7nzx627_9tbw374v3r1h0000gn/T/output
                                  ; required]
  --overwrite BOOLEAN             Controls whether to overwrite any existing
                                  DDL export files in the output path.
                                  [default: False; required]
  --git-repo TEXT                 Allows you to specify a git repository to
                                  push the output files to.  The repository
                                  must be accessible via SSH. Example:
                                  git@github.com:some-org/some-repo.git See: h
                                  ttps://docs.github.com/en/authentication/con
                                  necting-to-github-with-ssh/adding-a-new-ssh-
                                  key-to-your-github-account for more
                                  information on setting up SSH keys for
                                  GitHub.
  --git-branch TEXT               Specify the git branch to push to - if the
                                  --git-repo arg is used.  [default: main]
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
