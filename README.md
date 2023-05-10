# Invoice ETL pipeline


[![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)](https://docs.python.org/3/)
[![postgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![AirFlow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://www.postgresql.org/)


[![Open Issues](https://img.shields.io/github/issues-raw/Nomow/etl-invoice-data-from-csv-to-postgresql?style=for-the-badge)](https://github.com/Nomow/etl-invoice-data-from-csv-to-postgresql/issues)
[![Closed Issues](https://img.shields.io/github/issues-closed-raw/Nomow/etl-invoice-data-from-csv-to-postgresql?style=for-the-badge)](https://github.com/Nomow/etl-invoice-data-from-csv-to-postgresql/issues?q=is%3Aissue+is%3Aclosed)
[![Open Pulls](https://img.shields.io/github/issues-pr-raw/0xTheProDev/fastapi-clean-example?style=for-the-badge)](https://github.com/Nomow/etl-invoice-data-from-csv-to-postgresql/pulls)
[![Closed Pulls](https://img.shields.io/github/issues-pr-closed-raw/Nomow/etl-invoice-data-from-csv-to-postgresql?style=for-the-badge)](https://github.com/Nomow/etl-invoice-data-from-csv-to-postgresql/pulls?q=is%3Apr+is%3Aclosed)
[![Contributors](https://img.shields.io/github/contributors/Nomow/etl-invoice-data-from-csv-to-postgresql?style=for-the-badge)](https://github.com/Nomow/etl-invoice-data-from-csv-to-postgresql/graphs/contributors)
[![Activity](https://img.shields.io/github/last-commit/Nomow/etl-invoice-data-from-csv-to-postgresql?style=for-the-badge&label=most%20recent%20activity)](https://github.com/Nomow/etl-invoice-data-from-csv-to-postgresql/pulse)



## Description
ETL pipeline, using Python3, Airflow, Pandas, PostgreSQL.
Invoice data are extracted from CSV file, transformed, and loaded in PostgreSQL database.


## Installation:
All commands should be run from repositories root directory.
- Run the command from command prompt to create and start PostgreSQL container:
```sh
  $ docker run --name pgsql -e POSTGRES_PASSWORD=test123 -e POSTGRES_DB=invoice_database -p 5432:5432 -d postgres:13
  ```

- Create and activate Conda environment from ```environment.yml``` file:
```sh
  $ conda env create --name etl --file environment.yml 
  $ conda activate etl
```

- add environment variables:
  - In case using different credentials for PostgresSQL edit **AIRFLOW_CONN_DB_POSTGRES** :
```sh
  $ export AIRFLOW__CORE__LOAD_EXAMPLES=False
  $ export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
  $ export AIRFLOW_HOME=$(pwd)
  $ export AIRFLOW_CONN_DB_POSTGRES='postgresql+psycopg2://postgres:test123@localhost/invoice_database'
```


- Run migrations:
```sh
  $ alembic upgrade head 
```

- Run AirFlow:
```sh
  $ airflow db init
  $ airflow standalone
```

## Accessing AirFlow
AirFlow Web UI is accessing through [http://localhost:8080](http://localhost:8080)
- Username ```admin```
- Password - see the console after running ```airflow standalone``` command or see ```standalone_admin_password.txt``` in root directory, as it's auto generated.
- Name of DAG is ```invoice_etl_dag``` under tag ```etl```.

## Accessing PostgreSQL invoice database
- IP and Port ```localhost:5432```
- Username - ```postgres```
- Password - ```test123```
- Database name - ```invoice_database```
- table name of invoices - ```invoices```

## To Do's
- Automated tests
- CI pipeline
- dockerized version