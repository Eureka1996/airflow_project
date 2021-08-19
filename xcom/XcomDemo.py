# airflow task间进行参数传递

import datetime
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'hive',
    'depends_on_past': False,
    'ding_list': ['a1@tal.com','b1@tal.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date':datetime.datetime(2021,8,17,0,0,0)
}

dag = DAG(
    dag_id='test-NnZnmKMT-offline-test',
    default_args=default_args,
    description='',
    catchup=False,
    concurrency=1,
    schedule_interval='2 00 * * *')

def push_data(**context):
    value = datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')+'wufuqiang'
    print("设置的参数：%s"%value)
    print(str(context))
    context['ti'].xcom_push(key='test_key', value=value)


push_data_op = PythonOperator(
    task_id='push_data',
    python_callable=push_data,
    provide_context=True,
    dag=dag
)


def pull_data(**context):
    test_data = context['ti'].xcom_pull(key='test_key')
    print("获取到的参数：%s" % test_data)


pull_data_op = PythonOperator(
    task_id='pull_data',
    python_callable=pull_data,
    provide_context=True,
    dag=dag
)


t1 = BashOperator(
    task_id="t1",
    bash_command='echo "{{ ti.xcom_push(key="k1", value="v1") }}" "{{ti.xcom_push(key="k2", value="v2") }}"',
    dag=dag,
)
t2 = BashOperator(
    task_id="t2",
    on_success_callback=push_data,
    bash_command='echo "{{ ti.xcom_pull(key="k1") }}" "{{ ti.xcom_pull(key="k2") }}" "{{ti.xcom_pull(key="test_key_outer")}}"',
    dag=dag,
)

t1 >> t2

push_data_op >> pull_data_op