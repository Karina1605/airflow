from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow import Dataset
from airflow.models.baseoperator import chain
from pendulum import datetime
from files_hw_DE.transform_script import transfrom
import datetime
import pandas as pd
filePath = 'data/profit_table.csv'
savePath = 'data/flags_activity.csv'
@dag(
    dag_id="akchurina_karina_user_activity_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval = "0 0 */5 * *",
    catchup=False,
    doc_md=__doc__,
    default_args={"retries": 3}
)
def user_activity_dag():
    # read the updated data
    data = pd.read_csv(filePath)
    result = transfrom(data, datetime.datetime.now())
    #append results
    result.to_csv(f'flags_activity.csv', mode='a', index=False)

