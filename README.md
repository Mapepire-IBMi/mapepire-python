# mapepire-python
<div align="center">
<a href="https://github.com/Mapepire-IBMi/mapepire-python/actions">
    <img alt="CI" src="https://img.shields.io/github/actions/workflow/status/Mapepire-IBMi/mapepire-python/main.yml">
</a>
<a href="https://pypi.org/project/mapepire-python/">
    <img alt="PyPI" src="https://img.shields.io/pypi/v/mapepire-python">
</a>
<a href="https://github.com/Mapepire-IBMi/mapepire-python/blob/main/LICENSE">
    <img alt="License" src="https://img.shields.io/github/license/allenai/tango.svg?color=blue&cachedrop">
</a>
<br/>
</div>

- [mapepire-python](#mapepire-python)
  - [Overview](#overview)
  - [Setup](#setup)
    - [Install with `pip`](#install-with-pip)
    - [Server Component Setup](#server-component-setup)
  - [Example usage](#example-usage)
    - [Query and run](#query-and-run)
    - [Asynchronous Query Execution](#asynchronous-query-execution)
  - [Pooling (beta)](#pooling-beta)
- [Development Setup](#development-setup)
  - [Setup python virtual environment with pip and venv](#setup-python-virtual-environment-with-pip-and-venv)
    - [Create a new virtual environment](#create-a-new-virtual-environment)
      - [Unix/macOS](#unixmacos)
      - [Windows](#windows)
    - [Activate the virtual environment:](#activate-the-virtual-environment)
      - [Unix.macOS](#unixmacos-1)
      - [Windows](#windows-1)
      - [Unix/macOS](#unixmacos-2)
      - [Windows](#windows-2)
    - [Prepare pip](#prepare-pip)
      - [Unix/macOS](#unixmacos-3)
      - [Windows](#windows-3)
    - [Install Dependencies using `requirements-dev.txt`](#install-dependencies-using-requirements-devtxt)
      - [Unix/macOS](#unixmacos-4)
      - [Windows](#windows-4)
  - [Setup Python virtual environment with Conda](#setup-python-virtual-environment-with-conda)
    - [Create an environment from an environment-dev.yml file](#create-an-environment-from-an-environment-devyml-file)
      - [1. Activate the new environment:](#1-activate-the-new-environment)
      - [2. Verify the new environment was installed:](#2-verify-the-new-environment-was-installed)
  - [Run local test suite](#run-local-test-suite)



## Overview

<img src="images/mapepire-logo.png" alt="logo" width="200"/>

---


`mapepire-python` is a Python client implementation for [Mapepire](https://github.com/Mapepire-IBMi) that provides a simple interface for connecting to an IBM i server and running SQL queries. The client is designed to work with the [Mapepire Server Component](https://github.com/Mapepire-IBMi/mapepire-server)

## Setup

`mapepire-python` requires Python 3.9 or later.

### Install with `pip`

`mapepire-python` is available on [PyPi](https://pypi.org/project/mapepire-python/). Just Run

```bash
pip install mapepire-python
```

### Server Component Setup
To use mapire-python, you will need to have the Mapepire Server Component running on your IBM i server. Follow these instructions to set up the server component: [Mapepire Server Installation](https://mapepire-ibmi.github.io/guides/sysadmin/)

## Example usage

Setup the server credentials used to connect to the server. One way to do this is to create a `mapepire.ini` file in the root of your project with the following content:

```ini
[mapepire]
SERVER="SERVER"
PORT="PORT"
USER="USER"
PASSWORD="PASSWORD"
```

The following script sets up a `DaemonServer` object that will be used to connect with the Server Component. Then a single `SQLJob` is created to facilitate the connection from the client side.

```python
import configparser
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.types import DaemonServer

config = configparser.ConfigParser()
config.read('mapepire.ini')

creds = DaemonServer(
    host=config['mapepire']['SERVER'],
    port=config['mapepire']['PORT'],
    user=config['mapepire']['USER'],
    password=config['mapepire']['PASSWORD'],
    ignoreUnauthorized=True
)

with SQLJob(creds) as sql_job:
    with sql_job.query("select * from sample.employee") as query:
        result = query.run(rows_to_fetch=1)
        print(result)
```

Here is the output from the script above:

```json
{
  "id":"query3",
  "has_results":true,
  "update_count":-1,
  "metadata":{
    "column_count":14,
    "job":"330955/QUSER/QZDASOINIT",
    "columns":[
      {
        "name":"EMPNO",
        "type":"CHAR",
        "display_size":6,
        "label":"EMPNO"
      },
      {
        "name":"FIRSTNME",
        "type":"VARCHAR",
        "display_size":12,
        "label":"FIRSTNME"
      },
      {
        "name":"MIDINIT",
        "type":"CHAR",
        "display_size":1,
        "label":"MIDINIT"
      },
      {
        "name":"LASTNAME",
        "type":"VARCHAR",
        "display_size":15,
        "label":"LASTNAME"
      },
      {
        "name":"WORKDEPT",
        "type":"CHAR",
        "display_size":3,
        "label":"WORKDEPT"
      },
      {
        "name":"PHONENO",
        "type":"CHAR",
        "display_size":4,
        "label":"PHONENO"
      },
      {
        "name":"HIREDATE",
        "type":"DATE",
        "display_size":10,
        "label":"HIREDATE"
      },
      {
        "name":"JOB",
        "type":"CHAR",
        "display_size":8,
        "label":"JOB"
      },
      {
        "name":"EDLEVEL",
        "type":"SMALLINT",
        "display_size":6,
        "label":"EDLEVEL"
      },
      {
        "name":"SEX",
        "type":"CHAR",
        "display_size":1,
        "label":"SEX"
      },
      {
        "name":"BIRTHDATE",
        "type":"DATE",
        "display_size":10,
        "label":"BIRTHDATE"
      },
      {
        "name":"SALARY",
        "type":"DECIMAL",
        "display_size":11,
        "label":"SALARY"
      },
      {
        "name":"BONUS",
        "type":"DECIMAL",
        "display_size":11,
        "label":"BONUS"
      },
      {
        "name":"COMM",
        "type":"DECIMAL",
        "display_size":11,
        "label":"COMM"
      }
    ]
  },
  "data":[
    {
      "EMPNO":"000010",
      "FIRSTNME":"CHRISTINE",
      "MIDINIT":"I",
      "LASTNAME":"HAAS",
      "WORKDEPT":"A00",
      "PHONENO":"3978",
      "HIREDATE":"01/01/65",
      "JOB":"PRES",
      "EDLEVEL":18,
      "SEX":"F",
      "BIRTHDATE":"None",
      "SALARY":52750.0,
      "BONUS":1000.0,
      "COMM":4220.0
    }
  ],
  "is_done":false,
  "success":true
}

```
The results object is a JSON object that contains the metadata and data from the query. Here are the different fields returned:
- `id` field contains the query ID
- `has_results` field indicates whether the query returned any results
- `update_count` field indicates the number of rows updated by the query (-1 if the query did not update any rows)
- `metadata` field contains information about the columns returned by the query
- `data` field contains the results of the query 
- `is_done` field indicates whether the query has finished executing
- `success` field indicates whether the query was successful. 

In the ouput above, the query was successful and returned one row of data.

### Query and run 

To create and run a query in a single step, use the `query_and_run` method: 

```python
import configparser
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.types import DaemonServer

config = configparser.ConfigParser()
config.read('mapepire.ini')

creds = DaemonServer(
    host=config['mapepire']['SERVER'],
    port=config['mapepire']['PORT'],
    user=config['mapepire']['USER'],
    password=config['mapepire']['PASSWORD'],
    ignoreUnauthorized=True
)

with SQLJob(creds) as sql_job:
    # query automatically closed after running
    results = sql_job.query_and_run("select * from sample.employee", rows_to_fetch=1)
    print(result)
```

### Asynchronous Query Execution

The `PoolJob` object can be used to create and run queries asynchronously:

```python
import asyncio
import configparser
from mapepire_python.pool.pool_job import PoolJob
from mapepire_python.types import DaemonServer

config = configparser.ConfigParser()
config.read('mapepire.ini')

creds = DaemonServer(
    host=config['mapepire']['SERVER'],
    port=config['mapepire']['PORT'],
    user=config['mapepire']['USER'],
    password=config['mapepire']['PASSWORD'],
    ignoreUnauthorized=True
)

async def main():
    async with PoolJob(creds=creds) as pool_job:
        async with pool_job.query('select * from sample.employee') as query:
          res = await query.run(rows_to_fetch=1)

if __name__ == '__main__':
    asyncio.run(main())

```

To run a create and run a query asynchronously in a single step, use the `query_and_run` method:

```python
import asyncio
import configparser
from mapepire_python.pool.pool_job import PoolJob
from mapepire_python.types import DaemonServer

config = configparser.ConfigParser()
config.read('mapepire.ini')

creds = DaemonServer(
    host=config['mapepire']['SERVER'],
    port=config['mapepire']['PORT'],
    user=config['mapepire']['USER'],
    password=config['mapepire']['PASSWORD'],
    ignoreUnauthorized=True
)

async def main():
    async with PoolJob(creds=creds) as pool_job:
        res = await pool_job.query_and_run(rows_to_fetch=1)
        print(res)

if __name__ == '__main__':
    asyncio.run(main())

```


## Pooling (beta)

The `Pool` object can be used to create a pool of `PoolJob` objects to run queries concurrently. 

```python
import asyncio
import configparser
from mapepire_python.pool.pool_client import Pool, PoolOptions
from mapepire_python.types import DaemonServer

config = configparser.ConfigParser()
config.read('mapepire.ini')

creds = DaemonServer(
    host=config['mapepire']['SERVER'],
    port=config['mapepire']['PORT'],
    user=config['mapepire']['USER'],
    password=config['mapepire']['PASSWORD'],
    ignoreUnauthorized=True
)


async def main():
    async with Pool(
        options=PoolOptions(
            creds=creds,
            opts=None,
            max_size=5,
            starting_size=3
        )
    ) as pool:
      job_names = []
      resultsA = await asyncio.gather(
          pool.execute('values (job_name)'),
          pool.execute('values (job_name)'),
          pool.execute('values (job_name)')
      )
      job_names = [res['data'][0]['00001'] for res in resultsA]

      print(job_names)


if __name__ == '__main__':
    asyncio.run(main())

```
This script will create a pool of 3 `PoolJob` objects and run the query `values (job_name)` concurrently. The results will be printed to the console.

```bash
['004460/QUSER/QZDASOINIT', '005096/QUSER/QZDASOINIT', '005319/QUSER/QZDASOINIT']
```


# Development Setup

This guide provides instructions for setting up a Python virtual environment using either `venv` or `conda`.

## Setup python virtual environment with pip and venv

- Create and activate virtual environment
- Prepare pip
- Install packages from `requirements-dev.txt`

### Create a new virtual environment

**Note**: This applies to supported versions of Python 3.8 and higher

navigate to the project's directory and run the following command. This will create a new virtual environment in a local folder named `.venv`

```bash
cd mapepire-python/
```

#### Unix/macOS

```bash
python3 -m venv .venv
```

#### Windows

```bash
py -m venv .venv
```

The second argument is the location of the virtual environment, which will create a the virtual environment in the mapepire-python project root directory: `mapepire-python/.venv`

### Activate the virtual environment:

before installing the project dependencies, `activate` the virtual environment to put the environment-specific `python` and `pip` executables into your shell's `PATH`

#### Unix.macOS

```bash
source .venv/bin/activate
```

#### Windows

```bash
.venv\Scripts\activate
```

Confirm the virtual environment is activated, check the location of the Python interpreter:

#### Unix/macOS

```bash 
which python
```

#### Windows

```bash 
where python
```
Expected output should be:

```bash
.venv/bin/python     # Unix/macOS
.venv\Scripts\python # Windows
```

To deactivate the virtual environment, run:

```bash
deactivate
```

from the `mapepire-python` project directory

### Prepare pip

Make sure pip is up to date:

#### Unix/macOS

```bash
python3 -m pip install --upgrade pip
python3 -m pip --version
```

#### Windows

```bash
py -m pip install --upgrade pip
py -m pip --version
```

### Install Dependencies using `requirements-dev.txt`

Run the following to install the project dependencies:

#### Unix/macOS

```bash
python3 -m pip install -r requirements-dev.txt
```

#### Windows

```bash
py -m pip install -r requirements-dev.txt
```


## Setup Python virtual environment with Conda

First, install Conda if you haven't already by following the instructions in this [guide](https://conda.io/projects/conda/en/latest/user-guide/install/index.html). There are installers for macOS/Windows and Linux. I recommend the following installers for this project:

- [Miniconda](https://docs.anaconda.com/miniconda/)
  - Miniconda is a minimal installer provided by Anaconda.
- [Anaconda](https://www.anaconda.com/download)
  - Anaconda Distribution is a full featured installer that comes with a suite of packages for data science, as well as Anaconda Navigator, a GUI application for working with conda environments.
  
### Create an environment from an environment-dev.yml file

In a terminal, navigate to the `mapepire-python` project directory and run the following command:

```bash 
cd mapepire-python/

conda env create -f environment-dev.yml
```
The `conda env create` command will create a python environment called `mapepire-dev`.

#### 1. Activate the new environment: 
```bash
conda activate mapepire-dev
```

#### 2. Verify the new environment was installed:

```bash
conda env list
```
You can also use conda info --envs.

To deactivate, call:

```bash
conda deactivate
```
## Run local test suite

First, create a `pytest.ini` file in the `tests` directory. 

`tests/pytest.ini`

```ini
[pytest]
env =
    VITE_SERVER=IBMI_SERVER
    VITE_DB_USER=USER
    VITE_DB_PASS=PASS
```

Run the test suite from the `mapepire-python` directory:

```bash
# activate python development environment first

pytest tests/
```























