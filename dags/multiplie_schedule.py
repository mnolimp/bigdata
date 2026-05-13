import json
import requests
import tempfile
import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.hooks.base import BaseHook
from airflow.sdk import Asset
from airflow.operators.python import get_current_context

TEACHER_IDS = [1003026, 782898, 1001117, 36240, 1001142]

schedule_asset = Asset("hdfs://schedule/bronze")

@dag(
    dag_id='analyse_multiply_schedule',
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    tags=['schedule', 'airflow3']
)
def teachers_schedule_hdfs():

    @task(task_id='get_teachers')
    def get_teachers() -> list[int]:
        return TEACHER_IDS

    @task(task_id='process_teachers_schedule', trigger_rule="none_failed")
    def process_teacher_schedule(teacher_id: int) -> str:
        ctx = get_current_context()
        execution_date = ctx["data_interval_start"]

        api_conn = BaseHook.get_connection("omstu_schedule_api")
        extra = json.loads(api_conn.extra or "{}")
        base_url = extra["base_url"]

        start_date = execution_date.start_of("week").strftime("%Y.%m.%d")
        end_date = execution_date.end_of("week").strftime("%Y.%m.%d")

        api_url = f"{base_url}/api/schedule/person/{teacher_id}?start={start_date}&finish={end_date}&lng=1"

        print(f"Request: {api_url}")

        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()

            if not response.text or response.text.strip() == "":
                print(f"Empty response for teacher_id={teacher_id}")
                schedule_data = []
            else:
                try:
                    schedule_data = response.json()
                except json.JSONDecodeError:
                    print(f"Invalid JSON for teacher_id={teacher_id}")
                    schedule_data = []

            print(f"Got schedule for teacher_id={teacher_id}")

        except requests.exceptions.RequestException as e:
            print(f"API error for teacher_id={teacher_id}: {e}")
            schedule_data = []

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            json.dump(schedule_data, tmp_file, ensure_ascii=False, indent=2)
            tmp_file_path = tmp_file.name

        try:
            hdfs_dir = (
                f"/user/airflow/schedule/"
                f"year={execution_date.year}/"
                f"month={execution_date.month:02d}/"
                f"day={execution_date.day:02d}/"
                f"teacher_id={teacher_id}"
            )
            hdfs_file = f"{hdfs_dir}/schedule.json"

            hook = WebHDFSHook(webhdfs_conn_id="webhdfs_connection")
            client = hook.get_conn()
            client.makedirs(hdfs_dir)
            hook.load_file(source=tmp_file_path, destination=hdfs_file, overwrite=True)

            print(f"Saved to HDFS: {hdfs_file}")
            return hdfs_file

        finally:
            os.unlink(tmp_file_path)

    @task(task_id='publish_bronze_asset', outlets=[schedule_asset])
    def publish_bronze_asset(hdfs_files: list, *, outlet_events):
        ctx = get_current_context()
        execution_date = ctx["data_interval_start"]

        year  = execution_date.year
        month = execution_date.month
        day   = execution_date.day

        outlet_events[schedule_asset].extra = {
            "year": year,
            "month": month,
            "day": day,
        }
        print(f"Published bronze asset for {year}-{month:02d}-{day:02d}")

    teacher_ids = get_teachers()
    hdfs_files = process_teacher_schedule.expand(teacher_id=teacher_ids)
    publish_bronze_asset(hdfs_files)

teachers_schedule_hdfs()