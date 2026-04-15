from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random

import aiohttp
import asyncio
import json
import os 

start_date = "2026.02.02"
end_date = "2026.02.08"

API_URL = f"https://rasp.omgtu.ru/api/schedule/person/1003026?start={start_date}&finish={end_date}&lng=1"

CONFIG_FILE_PATH = "/opt/airflow/dags/configs/start_process.conf"

@dag(
    dag_id='analyse_schedule',
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    tags=['schedule', 'airflow3']
)

def generate_schedule_analyse_dag():

    start = EmptyOperator(task_id = 'Start')

    wait_for_config = PythonSensor(
        task_id="Wait_for_start_flag",
        python_callable=lambda: os.path.exists(CONFIG_FILE_PATH),
        poke_interval=10,
        timeout=60 * 60 * 6,
        mode="poke",
    )


    @task(task_id = 'Get_schedule_data')
    def get_schedule_data():
        async def fetch():
            async with aiohttp.ClientSession() as session:
                async with session.get(API_URL) as response:
                    return await response.json()

        schedule_data = asyncio.run(fetch())
        return schedule_data

    
    @task.branch(task_id = 'Check_if_empty')
    def check_if_empty(schedule: list):
        if schedule:
            return 'Save_json'
        else: 
            return 'Take_a_break'

    
    @task(task_id = 'Save_json')
    def save_json(schedule: list, ds: str):
        output_dir = "/opt/airflow/dags/data/"
        os.makedirs(output_dir, exist_ok = True)

        file_path = f"{output_dir}schedule_{ds}.json"

        with open(file_path, "w", encoding = "utf-8") as file:
            json.dump(schedule, file, ensure_ascii = False, indent=4)

        print(f"File saved in {file_path}")

    @task(task_id = "Take_a_break")
    def take_a_break():
        print("На этой неделе пар у коллеги нет, можно отдыхать")


    final = EmptyOperator(
        task_id='End',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    start >> wait_for_config

    schedule_data = get_schedule_data()

    branch_result = check_if_empty(schedule_data)

    save = save_json(schedule_data)
    kitkat = take_a_break()             #have a break - kitkat xd

    wait_for_config >> schedule_data >> branch_result

    branch_result >> [save, kitkat] >> final

generate_schedule_analyse_dag()