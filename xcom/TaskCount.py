



def count_failed_task(**context):
    source_tables = context['source_tables'].split(',')
    with closing(settings.Session()) as session:
        all_tasks = session.query(models.TaskInstance).filter(
            models.TaskInstance.dag_id == context["dag"].dag_id,
            models.TaskInstance.execution_date == context["execution_date"],
            models.TaskInstance.state.in_([State.FAILED, State.UPSTREAM_FAILED])
        ).all()

        for t in all_tasks:
            print("失败的任务："+t.task_id)

        my_datax_tasks = [val for val in all_tasks if val.task_id.startswith(tuple(source_tables))]

        if len(my_datax_tasks) > 0 :
            raise Exception("上游出现任务执行失败。")


# 尾结点，空节点
test_odsdb__ods_ms_test_create_merge_test5_fd=PythonOperator(
    trigger_rule="all_done",
    retries=0,
    python_callable=count_failed_task,
    provide_context=True,
    op_kwargs={'source_tables': 'test,wufuqiang', 'key2': 'value2'},
    task_id="test",
    dag=dag
)