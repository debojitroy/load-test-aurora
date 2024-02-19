import os
import datetime
from dotenv import load_dotenv
import random
import logging
from statistics import median, mean, stdev
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg_pool import ConnectionPool
from .timer import Timer


load_dotenv()

db_host = os.environ["DB_READONLY_HOST"]
db_user = os.environ["DB_USER"]
db_password = os.environ["DB_PASSWORD"]
db_port = os.environ["DB_PORT"]
db_database = os.environ["DB_DATABASE"]
threads = int(os.environ["LOAD_THREADS"])

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logging.getLogger("psycopg.pool").setLevel(logging.INFO)

pool = ConnectionPool(
    conninfo=f"host={db_host} dbname={db_database} user={db_user} password={db_password} port={db_port}",
    min_size=threads,
)
pool.wait()
logging.info("pool ready")

time_counter = []
start_date = datetime.datetime(2003, 8, 1, 12, 4, 5)


def run_query():
    with pool.connection() as conn:
        t = Timer()
        t.start()
        n = str(random.randint(1, 500_000))
        start = start_date + datetime.timedelta(days=random.randint(0, 300))
        end = start_date + datetime.timedelta(days=random.randint(301, 700))
        conn.execute(
            """SELECT * from readings where nmi = %s and interval_date > %s and interval_date < %s""",
            (n, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
        ).fetchall()
        time_counter.append(t.stop())


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(run_query) for _ in range(threads)]
        for future in as_completed(futures):
            future.result()

        max_time = max(time_counter)
        min_time = min(time_counter)
        mean_time = mean(time_counter)
        median_time = median(time_counter)
        stdev_time = stdev(time_counter)

        with open("./output/load_test_result.csv", mode="a") as f:
            f.write("\n".join(f"{threads},{max_time:0.4f},{min_time:0.4f},{mean_time:0.4f},{median_time:0.4f},{stdev_time:0.4f}"))
            f.write("\n")

        print("-------------------------------Results----------------------------")
        print(f"Threads: {threads}")
        print(f"Max: {max_time:0.4f} seconds")
        print(f"Min: {min_time:0.4f} seconds")
        print(f"Mean: {mean_time:0.4f} seconds")
        print(f"Median: {median_time:0.4f} seconds")
        print(f"Std Dev: {stdev_time:0.4f} seconds")
        print("-----------------------------------------------------------")
