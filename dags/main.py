import json
import requests
import tempfile
import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.hooks.base import BaseHook

TEACHER_IDS = [1003026, 782898, 1001117, 36240, 1001142]

@dag(
    dag_id="teachers_schedule_hdfs",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "student",
        "retries": 1,
    },
    tags=["data_lake", "hdfs", "schedule"],
)
def teachers_schedule_hdfs():
    """
    ### DAG для сбора расписания преподавателей в Data Lake (HDFS)
    """

    @task
    def get_teachers() -> list[int]:
        """Возвращает список ID преподавателей."""
        return TEACHER_IDS

    @task(trigger_rule="none_failed")
    def process_teacher_schedule(teacher_id: int, **context) -> str:
        """
        Получает расписание преподавателя за неделю через API ОмГТУ и сохраняет в HDFS.
        """
        execution_date = context["logical_date"]

        api_conn = BaseHook.get_connection("omstu_schedule_api")
        extra = json.loads(api_conn.extra or "{}")
        base_url = extra["base_url"]

        start_date = execution_date.start_of("week").strftime("%Y-%m-%d")
        end_date = execution_date.end_of("week").strftime("%Y-%m-%d")

        api_url = f"{base_url}/schedule/person/{teacher_id}?start={start_date}&finish={end_date}&lng=1"

        print(f"Запрос к API: {api_url}")

        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            schedule_data = response.json()
            print(f"Получено расписание для преподавателя ID={teacher_id}")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе к API: {e}")
            raise

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
            
            print(f"Успешно сохранено в HDFS: {hdfs_file}")
            return hdfs_file
            
        except Exception as e:
            print(f"Ошибка при сохранении в HDFS: {e}")
            raise
        finally:
            os.unlink(tmp_file_path)

    # Dynamic Task Mapping
    teacher_ids = get_teachers()
    process_teacher_schedule.expand(teacher_id=teacher_ids)

teachers_schedule_hdfs()