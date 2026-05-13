from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import Asset
from airflow.operators.python import get_current_context
from airflow.hooks.base import BaseHook
import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when
from pyspark.sql.types import IntegerType, LongType

schedule_asset = Asset("hdfs://schedule/bronze")

@dag(
    dag_id='silver_and_clickhouse',
    schedule=[schedule_asset],
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['schedule', 'airflow3'],
)
def silver_and_clickhouse():

    def get_date_from_context():
        ctx = get_current_context()
        triggering_events = ctx.get("triggering_asset_events", {})
        events = list(triggering_events.values())[0]
        extra = events[-1].extra or {}
        return extra["year"], extra["month"], extra["day"]

    @task(task_id='json_to_parquet')
    def json_to_parquet():
        year, month, day = get_date_from_context()

        bronze_path = (
            f"hdfs://namenode:9000/user/airflow/schedule/"
            f"year={year}/month={month:02d}/day={day:02d}/"
            f"*/schedule.json"
        )
        silver_path = (
            f"hdfs://namenode:9000/user/airflow/silver/schedule/"
            f"year={year}/month={month:02d}/day={day:02d}"
        )

        print(f"Processing date: {year}-{month:02d}-{day:02d}")

        spark = (
            SparkSession.builder
            .appName("bronze_to_silver")
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
            .getOrCreate()
        )

        try:
            raw_df = spark.read.option("multiline", "true").json(bronze_path)

            if raw_df.rdd.isEmpty():
                print("No data found for this date, skipping.")
                return

            lessons_df = (
                raw_df
                .select(
                    col("lecturerOid").cast(LongType()).alias("lecturer_oid"),
                    col("lecturer_title").alias("lecturer_name"),
                    col("lecturer_rank"),
                    col("discipline"),
                    col("kindOfWork").alias("kind_of_work"),
                    col("date").alias("lesson_date"),
                    col("dayOfWeekString").alias("day_of_week"),
                    col("lessonNumberStart").cast(IntegerType()).alias("lesson_num"),
                    col("beginLesson").alias("time_start"),
                    col("endLesson").alias("time_end"),
                    col("auditorium"),
                    col("building"),
                    col("auditoriumAmount").cast(IntegerType()).alias("auditorium_capacity"),
                    coalesce(
                        when(col("stream").isNotNull() & (col("stream") != ""), col("stream")),
                        when(col("group").isNotNull() & (col("group") != ""), col("group")),
                        when(col("subGroup").isNotNull() & (col("subGroup") != ""), col("subGroup")),
                        lit("(не указана)")
                    ).alias("groups"),
                    lit(f"{year}-{month:02d}-{day:02d}").alias("partition_date"),
                )
                .dropDuplicates(["lecturer_oid", "lesson_date", "lesson_num", "auditorium"])
                .filter(col("lesson_date").isNotNull())
            )

            lessons_df.write.mode("overwrite").parquet(silver_path)

            count = lessons_df.count()
            print(f"Saved {count} rows -> {silver_path}")

        finally:
            spark.stop()

    @task(task_id='parquet_to_clickhouse')
    def parquet_to_clickhouse():
        year, month, day = get_date_from_context()
        partition_date = f"{year}-{month:02d}-{day:02d}"

        silver_path = (
            f"hdfs://namenode:9000/user/airflow/silver/schedule/"
            f"year={year}/month={month:02d}/day={day:02d}/*.parquet"
        )

        print(f"Loading to ClickHouse partition: {partition_date}")

        conn = BaseHook.get_connection("clickhouse_default")
        password = conn.password or "airflow"

        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="default",
            password=password,
            database="rasp_omgtu",
        )

        client.command("""
            CREATE TABLE IF NOT EXISTS rasp_omgtu.schedule (
                lecturer_oid Int64,
                lecturer_name String,
                lecturer_rank String,
                discipline String,
                kind_of_work String,
                lesson_date String,
                day_of_week String,
                lesson_num Int32,
                time_start String,
                time_end String,
                auditorium String,
                building String,
                auditorium_capacity Int32,
                groups String,
                partition_date Date
            )
            ENGINE = MergeTree()
            PARTITION BY partition_date
            ORDER BY (lecturer_oid, lesson_date, lesson_num)
        """)

        client.command(
            f"ALTER TABLE rasp_omgtu.schedule DROP PARTITION '{partition_date}'"
        )

        client.command(f"""
            INSERT INTO rasp_omgtu.schedule
            SELECT
                lecturer_oid,
                lecturer_name,
                lecturer_rank,
                discipline,
                kind_of_work,
                lesson_date,
                day_of_week,
                lesson_num,
                time_start,
                time_end,
                auditorium,
                building,
                auditorium_capacity,
                groups,
                toDate(partition_date) AS partition_date
            FROM hdfs(
                '{silver_path}',
                'Parquet'
            )
        """)

        print(f"Loaded partition {partition_date} into ClickHouse")

    json_to_parquet() >> parquet_to_clickhouse()


silver_and_clickhouse()