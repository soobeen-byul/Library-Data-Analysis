from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import pendulum
from airflow.models.variable import Variable

local_tz = pendulum.timezone("Asia/Seoul")

LBR_SPARK_PATH = Variable.get("LBR_SPARK_PATH")

default_args = {
    'owner': 'sub',
    'depends_on_past': False,
    'start_date': datetime(year=2023, month=6, day=26, hour=0, minute=0, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id = 'sub-spark-library',
    description = 'pd24 mario sub spark pipeline : library data analysis',
    tags = ['spark', 'sub', 'library'],
    max_active_runs = 1,
    concurrency = 10,
    schedule_interval = '30 6 * * *',
    user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
    default_args = default_args
)
# Define the BashOperator task
# https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
check_execute_task = BashOperator(
    task_id='check.execute',
    bash_command="""
        echo "date                            => `date`"        
        echo "logical_date                    => {{logical_date}}"
        echo "execution_date                  => {{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
        echo "next_execution_date             => {{next_execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
        echo "prev_execution_date             => {{prev_execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
        echo "local_dt(execution_date)        => {{local_dt(execution_date)}}"
        echo "local_dt(next_execution_date)   => {{local_dt(next_execution_date)}}"
        echo "local_dt(prev_execution_date)   => {{local_dt(prev_execution_date)}}"
        echo "===================================================================="
        echo "data_interval_start             => {{data_interval_start}}"
        echo "data_interval_end               => {{data_interval_end}}"
        echo "ds => {{ds}}"
        echo "ds_nodash => {{ds_nodash}}"
        echo "ds_nodash => {{ds_nodash}}"
        echo "ts  => {{ts}}"
        echo "ts_nodash_with_tz  => {{ts_nodash_with_tz}}"
        echo "prev_data_interval_start_success  => {{prev_data_interval_start_success}}"
        echo "prev_data_interval_end_success => {{prev_data_interval_end_success}}"
        echo "prev_data_interval_end_success => {{prev_data_interval_end_success}}"
        echo "prev_start_date_success => {{prev_start_date_success}}"
        echo "dag => {{dag}}"
        echo "task => {{task}}"
        echo "macros => {{macros}}"
        echo "task_instance => {{task_instance}}"
        echo "ti => {{ti}}"
        echo "====================================================================="
        echo "dag_run.logical_date => {{dag_run.logical_date}}"
        echo "execution_date => {{execution_date}}"
        echo "====================================================================="
        #2020-11-11 형식의 날짜 반환
        echo "exe_kr = {{execution_date.add(hours=9).strftime("%Y-%m-%d")}}"
        #20201212 형식의 날짜 반환
        echo "exe_kr_nodash = {{execution_date.add(hours=9).strftime("%Y%m%d")}}"
        echo "====================================================================="
        """,
    dag=dag
)

# SPARK
spark_task_cmd1 = f"""
                        $SPARK_HOME/bin/spark-submit --master spark://DESKTOP-2G4ORF2.:7077  \
                        --name sample_job_em512_i2 \
                        --executor-memory 512m \
                        --total-executor-cores 2 \
                        {LBR_SPARK_PATH}/spark-step-1.py
                        """

spark_task_1 = BashOperator(
   task_id='Extract.base_file',
   bash_command=spark_task_cmd1 ,
    dag=dag
  )

spark_task_cmd2 = f"""
                        $SPARK_HOME/bin/spark-submit --master spark://DESKTOP-2G4ORF2.:7077  \
                        --name sample_job_em512_i2 \
                        --executor-memory 512m \
                        --total-executor-cores 2 \
                        {LBR_SPARK_PATH}/spark-step-2.py
                        """

spark_task_2 = BashOperator(
   task_id='Merge.3table',
   bash_command=spark_task_cmd2 ,
    dag=dag
  )


# DummyOperator
start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)

# WF
start_task >> check_execute_task
check_execute_task >> spark_task_1 >> spark_task_2 >> end_task