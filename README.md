# Load Test Aurora

Sample code to populate Aurora Postgres and load test

## Install Dependencies

```shell
pipenv install
```

## Loading Tables

### Creating Tables

```sql
CREATE TABLE readings (
  nmi varchar(10) NOT NULL,
  interval_date date NOT NULL,
  direction char(1) NOT NULL,
  nmi_day_id bigint[] NOT NULL,
  quantity decimal NOT NULL,
  intervals decimal[] NOT NULL,
  PRIMARY KEY (nmi, interval_date, direction),
)
```

### Creating ENV File

Create a `.env` file in the `project` root. Add the following keys.

```env
FILE_COUNT=
NMI_PER_FILE=
BUCKET_NAME=
WORKER_THREADS=
DB_HOST=
DB_READONLY_HOST=
DB_USER=
DB_PASSWORD=
DB_PORT=
DB_DATABASE=
LOAD_THREADS=
```

### Bulk Loading the tables

The program uses `aws_s3.table_import_from_s3` to bulk upload data to `Aurora Postgres`
Follow the [setup](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_PostgreSQL.S3Import.html) to enable the `extension` and correct `permissions`

```shell
pipenv run python -m generate.load_parallel
```

## Load testing

Once the tables are loaded. Change the `env` variable `LOAD_THREADS` to load test the database with varying loads.

```shell
pipenv run python -m load_test.load
```
