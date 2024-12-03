import pytest
import os
import csv
from unittest.mock import patch, MagicMock
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from read_csv_write_db import generate_data, load_data_to_postgres, log_record_count


# Тест: generate_data
def test_generate_data(tmp_path, mocker):
    temp_dir = tmp_path / "tmp"
    temp_dir.mkdir()
    file_path = temp_dir / "data.csv"

    # Создаем Mock объекта TaskInstance
    mock_ti = MagicMock()
    mock_ti.xcom_push = MagicMock()

    # Выполнение функции с mock TaskInstance
    generate_data(output_path=str(file_path), ti=mock_ti)

    # Проверка файла
    assert os.path.exists(file_path), "Файл не был создан"
    with open(file_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        assert len(rows) == 10, "Неправильное количество строк"

    # Проверка вызова xcom_push
    mock_ti.xcom_push.assert_called_once_with(key="file_path", value=str(file_path))


# Тест: load_data_to_postgres
def test_load_data_to_postgres(mocker):
    # Подготовка данных
    file_path = "/tmp/processed_data/data.csv"
    rows = [{"id": i, "value": f"value_{i}"} for i in range(1, 11)]

    # Создаем Mock объекта TaskInstance
    mock_ti = MagicMock()
    mock_ti.xcom_pull = MagicMock(return_value=file_path)

    # Создание временного файла
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["id", "value"])
        writer.writeheader()
        writer.writerows(rows)

    # Мок PostgresHook
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch.object(PostgresHook, "get_conn", return_value=mock_connection)

    # Выполнение функции
    load_data_to_postgres(ti=mock_ti)

    # Проверка SQL-запросов
    truncate_call = mock_cursor.execute.call_args_list[0]
    insert_call = mock_cursor.execute.call_args_list[1]

    assert "TRUNCATE TABLE test_table" in truncate_call[0][0], "Таблица не очищается"
    assert "INSERT INTO test_table" in insert_call[0][0], "Данные не вставляются"

    # Проверка вызова xcom_push для количества записей
    mock_ti.xcom_push.assert_called_once_with(key="record_count", value=10)


# Тест: log_record_count
def test_log_record_count(mocker):
    # Создаем mock для TaskInstance
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = 10

    # Мок логгера
    mock_logger = mocker.patch("read_csv_write_db.logger.info")

    # Выполнение функции
    log_record_count(ti=mock_ti)

    # Проверка логирования
    mock_logger.assert_called_once_with("Количество загруженных записей: 10")
