import os
import csv
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# Дефолтные параметры DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
}


# П.1. Генерация тестовых данных и записать их в файл /tmp/data.csv
def generate_data(output_path="/tmp/data.csv", **kwargs):
    data = [{"id": i, "value": f"value_{i}"} for i in range(1, 11)]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)  # Создание папки, если её нет
    with open(output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["id", "value"])
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"Сгенерированные данные сохранены в файл {output_path}: {data}")

    # Передача пути до файла из п.1 в оператор в п.2
    kwargs['ti'].xcom_push(key="file_path", value=output_path)


# П.2. Выполнен в виде BashOperator


# П.3. Загрузка данных в Postgres в предварительно созданную таблицу
# CREATE TABLE test_table (
#     id INT PRIMARY KEY,
#     value TEXT
# );
def load_data_to_postgres(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='move_file', key='return_value')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_dags')

    if not os.path.exists(file_path):
        logger.error(f"Файл {file_path} не найден.")
        raise FileNotFoundError(f"Файл {file_path} не найден.")

    with open(file_path, "r") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = [row for row in reader]

    # Очистка таблицы и вставка данных
    truncate_query = "TRUNCATE TABLE test_table;"
    insert_query = """
    INSERT INTO test_table (id, value)
    VALUES (%s, %s)
    """
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    logger.info("Очищаем таблицу...")
    cursor.execute(truncate_query)

    logger.info("Загружаем данные в базу данных...")
    for row in rows:
        cursor.execute(insert_query, (row["id"], row["value"]))

    connection.commit()
    cursor.close()
    logger.info(f"Загружено {len(rows)} записей в базу данных.")
    # Передача количества записей из п.3 в п.4
    kwargs['ti'].xcom_push(key="record_count", value=len(rows))


# П.4. Логирование количества записей
def log_record_count(**kwargs):
    record_count = kwargs['ti'].xcom_pull(task_ids='load_to_postgres', key='record_count')
    logger.info(f"Количество загруженных записей: {record_count}")


# Определение DAG
with DAG(
        "read_csv_write_db",
        default_args=default_args,
        description="Пайплайн для записи данных в файл и последующей их загрузке в Postgres",
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
) as dag:

    generate_random_data_task = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
        provide_context=True,
    )

    move_file_task = BashOperator(
        task_id="move_file",
        bash_command="mkdir -p /tmp/processed_data "
                     "&& mv {{ ti.xcom_pull(task_ids='generate_data', key='file_path') }} /tmp/processed_data/data.csv "
                     "&& echo /tmp/processed_data/data.csv",
        do_xcom_push=True,
    )

    load_to_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_data_to_postgres,
        provide_context=True,
    )

    log_count_task = PythonOperator(
        task_id="log_count",
        python_callable=log_record_count,
        provide_context=True,
    )

    generate_random_data_task >> move_file_task >> load_to_postgres_task >> log_count_task
