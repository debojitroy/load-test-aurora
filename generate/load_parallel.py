import os
import random
import datetime
import gzip
import shutil
import concurrent.futures
from dotenv import load_dotenv
import boto3
import psycopg


load_dotenv()

DELIMITER = "|"
day_ids = {random.randrange(100_000_000), random.randrange(100_000_000)}
intervals = "{" + ",".join([str(i) for i in range(10)]) + "}"
nmi_day_ids = "{1, 2}"

number_of_files = int(os.environ["FILE_COUNT"])
number_of_nmi_per_file = int(os.environ["NMI_PER_FILE"])
worker_threads = int(os.environ["WORKER_THREADS"])
bucket_name = os.environ["BUCKET_NAME"]

db_host = os.environ["DB_HOST"]
db_user = os.environ["DB_USER"]
db_password = os.environ["DB_PASSWORD"]
db_port = os.environ["DB_PORT"]
db_database = os.environ["DB_DATABASE"]

start_date = datetime.datetime(2003, 8, 1, 12, 4, 5)


def upload_file(file_name, bucket, object_name, ExtraArgs=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(file_name, bucket, object_name, ExtraArgs=ExtraArgs)
    except Exception as e:
        print(e)
        raise
    return True


def load_data_for_file(file_number, number_of_nmi_per_file):
    # Generate the records
    print(f"Preparing File: ./output/readings_{file_number}.csv")
    for nmi in range(
        file_number * number_of_nmi_per_file + 1,
        (file_number + 1) * number_of_nmi_per_file,
    ):
        records = []

        for reading_date in range(start_date.toordinal(), start_date.toordinal() + 730):
            for direction in "I", "E":
                day = datetime.date.fromordinal(reading_date)

                line = f"{nmi}|{day.strftime('%Y-%m-%d')}|{direction}|{day_ids}|{random.randrange(100, 500)}|{intervals}"
                records.append(line)

        # Write to files
        with open(f"./output/readings_{file_number}.csv", mode="a") as f:
            f.write("\n".join(records))
            f.write("\n")

    # Gzip the files
    print(f"GZipping File: ./output/readings_{file_number}.csv")
    with open(f"./output/readings_{file_number}.csv", "rb") as f_in:
        with gzip.open(f"./output/readings_{file_number}.csv.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Delete the uncompressed file
    os.remove(f"./output/readings_{file_number}.csv")

    print(f"Uploading to S3: ./output/readings_{file_number}.csv.gz")
    upload_file(
        f"./output/readings_{file_number}.csv.gz",
        bucket_name,
        f"readings_{file_number}.csv.gz",
        ExtraArgs={"ContentEncoding": "gzip"},
    )

    # Delete the file
    print(f"Successfully Uploaded to S3: ./output/readings_{file_number}.csv.gz")
    os.remove(f"./output/readings_{file_number}.csv.gz")

    # Load to DB
    with psycopg.connect(
        f"host={db_host} dbname={db_database} user={db_user} password={db_password} port={db_port}"
    ) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                print("Setting Sync Commit to off..")

                cur.execute("SET LOCAL synchronous_commit TO OFF;")

                print(f"Loading to DB: readings_{file_number}.csv.gz")
                cur.execute(
                    """
                    SELECT aws_s3.table_import_from_s3(
                    'readings',
                    'nmi,interval_date,direction,nmi_day_id,quantity,intervals',
                    'DELIMITER ''|''',
                    aws_commons.create_s3_uri(%s, %s, 'ap-southeast-2')
                    );
                    """,
                    (bucket_name, f"readings_{file_number}.csv.gz"),
                )

                print(cur.fetchone())


if __name__ == "__main__":
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=worker_threads)

    for i in range(number_of_files):
        pool.submit(load_data_for_file, i, number_of_nmi_per_file)

    pool.shutdown(wait=True)
