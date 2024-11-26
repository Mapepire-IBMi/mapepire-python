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

<details>
  <summary> Table of Contents </summary>

- [mapepire-python](#mapepire-python)
  - [Overview](#overview)
  - [Setup](#setup)
    - [Install with `pip`](#install-with-pip)
    - [Server Component Setup](#server-component-setup)
- [Connection options](#connection-options)
  - [1. Using the `DaemonServer` object](#1-using-the-daemonserver-object)
  - [2. Passing the connection details as a dictionary](#2-passing-the-connection-details-as-a-dictionary)
  - [3. Using a config file (`.ini`) to store the connection details](#3-using-a-config-file-ini-to-store-the-connection-details)
- [Usage](#usage)
  - [1. Using the `SQLJob` object to run queries synchronously](#1-using-the-sqljob-object-to-run-queries-synchronously)
    - [Query and run](#query-and-run)
  - [2. Using the `PoolJob` object to run queries asynchronously](#2-using-the-pooljob-object-to-run-queries-asynchronously)
  - [3. Using the `Pool` object to run queries "concurrently"](#3-using-the-pool-object-to-run-queries-concurrently)
  - [4. Using PEP 249 Implementation](#4-using-pep-249-implementation)
    - [`fetchmany()` and `fetchall()` methods](#fetchmany-and-fetchall-methods)
  - [PEP 249 Asynchronous Implementation](#pep-249-asynchronous-implementation)
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

</details>

## Overview

<img src="images/mapepire-logo.png" alt="logo" width="200"/>

---


`mapepire-python` is a Python client implementation for [Mapepire](https://github.com/Mapepire-IBMi) that provides a simple interface for connecting to an IBM i server and running SQL queries. The client is designed to work with the [Mapepire Server Component](https://github.com/Mapepire-IBMi/mapepire-server)

## Setup

`mapepire-python` requires Python 3.10 or later.

 > [!NOTE]
 >  New websocket Implementation: As of version 0.2.0, `mapepire-python` uses the `websockets` library for websocket connections. If you are upgrading from a previous version, make sure to update your dependecies. The `websocket-client` library is no longer supported.
 > - To update run `pip install -U mapepire-python`
 >
 > - More info on [websockets](https://websockets.readthedocs.io/en/stable/)


### Install with `pip`

`mapepire-python` is available on [PyPi](https://pypi.org/project/mapepire-python/). Just Run

```bash
pip install mapepire-python
```

### Server Component Setup
To use mapire-python, you will need to have the Mapepire Server Component running on your IBM i server. Follow these instructions to set up the server component: [Mapepire Server Installation](https://mapepire-ibmi.github.io/guides/sysadmin/)

   
# Connection options

There are three ways to configure mapepire server connection details using `mapepire-python`:

1. Using the `DaemonServer` object
2. Passing the connection details as a dictionary
3. Using a config file (`.ini`) to store the connection details

## 1. Using the `DaemonServer` object

to use the `DaemonServer` object, you will need to import the `DaemonServer` class from the `mapepire_python.data_types` module:

```python
from mapepire_python.data_types import DaemonServer

creds = DaemonServer(
    host="SERVER",
    port="PORT",
    user="USER",
    password="PASSWORD",
    ignoreUnauthorized=True
)
```

Once you have created the `DaemonServer` object, you can pass it to the `SQLJob` object to connect to the mapepire server:

```python
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import DaemonServer

creds = DaemonServer(
    host="SERVER",
    port="PORT",
    user="USER",
    password="PASSWORD",
    ignoreUnauthorized=True
)

job = SQLJob(creds)
```

## 2. Passing the connection details as a dictionary

You can also use a dictionary to configure the connection details:

```python
from mapepire_python.client.sql_job import SQLJob

creds = {
  "host": "SERVER",
  "port": "port",
  "user": "USER",
  "password": "PASSWORD",
}

job = SQLJob(creds)
```

this is a convenient way to pass the connection details to the mapepire server.

## 3. Using a config file (`.ini`) to store the connection details


If you use a config file (`.ini`), you can pass the path to the file as an argument:

First create a `mapepire.ini` file in the root of your project with the following required fields:

```ini title=mapepire.ini
[mapepire]
SERVER="SERVER"
PORT="PORT"
USER="USER"
PASSWORD="PASSWORD"
```

Then you can create a `SQLJob` object by passing the path to the `.ini` file which will handle the connection details


```python
from mapepire_python.client.sql_job import SQLJob

job = SQLJob("./mapepire.ini", section="mapepire")
```

The `section` argument is optional and allows you to specify a specific section in the `.ini` file where the connection details are stored. This allows you to store multiple connection details to different systems in the same file. If you do not specify a `section`, the first section in the file will be used. 


# Usage

Depending on your setup and use case, you can choose the most convenient way to configure the connection details. The following usage examples are compatible with all three connection options detailed above. For simplicity, we assume there is a `mapepire.ini` file in the root of the project with the connection details.


There are four main ways to run queries using `mapepire-python`:
1.  Using the `SQLJob` object to run queries synchronously
2.  Using the `PoolJob` object to run queries asynchronously
3.  Using the `Pool` object to run queries "concurrently"
4.  Using PEP 249 Implementation



## 1. Using the `SQLJob` object to run queries synchronously

```python
from mapepire_python.client.sql_job import SQLJob

with SQLJob("./mapepire.ini") as sql_job:
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
from mapepire_python.client.sql_job import SQLJob

with SQLJob("./mapepire.ini") as sql_job:
    # query automatically closed after running
    results = sql_job.query_and_run("select * from sample.employee", rows_to_fetch=1)
    print(result)
```

## 2. Using the `PoolJob` object to run queries asynchronously

The `PoolJob` object can be used to create and run queries asynchronously:

```python
import asyncio
from mapepire_python.pool.pool_job import PoolJob

async def main():
    async with PoolJob("./mapepire.ini") as pool_job:
        async with pool_job.query('select * from sample.employee') as query:
          res = await query.run(rows_to_fetch=1)

if __name__ == '__main__':
    asyncio.run(main())

```

To run a create and run a query asynchronously in a single step, use the `query_and_run` method:

```python
import asyncio
from mapepire_python.pool.pool_job import PoolJob

async def main():
    async with PoolJob("./mapepire.ini") as pool_job:
        res = await pool_job.query_and_run("select * from sample.employee", rows_to_fetch=1)
        print(res)

if __name__ == '__main__':
    asyncio.run(main())

```


## 3. Using the `Pool` object to run queries "concurrently"

The `Pool` object can be used to create a pool of `PoolJob` objects to run queries concurrently. 

```python
import asyncio
from mapepire_python.pool.pool_client import Pool, PoolOptions

async def main():
    async with Pool(
        options=PoolOptions(
            creds="./mapepire.ini",
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

## 4. Using PEP 249 Implementation

PEP 249 is the Python Database API Specification v2.0. The `mapepire-python` client provides a PEP 249 implementation that allows you to use the `Connection` and `Cursor` objects to interact with the Mapepire server. Like the examples above, we can pass the `mapepire.ini` file to the `connect` function to create a connection to the server:

```python
from mapepire_python import connect

with connect("./mapepire.ini") as conn:
    with conn.execute("select * from sample.employee") as cursor:
        result = cursor.fetchone()
        print(result)
```

### `fetchmany()` and `fetchall()` methods

The `Cursor` object provides the `fetchmany()` and `fetchall()` methods to fetch multiple rows from the result set:

```python
with connect("./mapepire.ini") as conn:
    with conn.execute("select * from sample.employee") as cursor:
        results = cursor.fetchmany(size=2)
        print(results)
```
---

```python
with connect("./mapepire.ini") as conn:
    with conn.execute("select * from sample.employee") as cursor:
        results = cursor.fetchall()
        print(results)
```

## PEP 249 Asynchronous Implementation

The PEP 249 implementation also provides an asynchronous interface for running queries. The `connect` function returns an asynchronous context manager that can be used with the `async with` statement:

```python
import asyncio
from mapepire_python.asycnio import connect

async def main():
    async with connect("./mapepire.ini") as conn:
        async with await conn.execute("select * from sample.employee") as cursor:
            result = await cursor.fetchone()
            print(result)
            
if __name__ == '__main__':
    asyncio.run(main())
```


# Development Setup

This guide provides instructions for setting up a Python virtual environment using either `venv` or `conda`.

## Setup python virtual environment with pip and venv

- Create and activate virtual environment
- Prepare pip
- Install packages from `requirements-dev.txt`

### Create a new virtual environment

**Note**: This applies to supported versions of Python 3.10 and higher

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























