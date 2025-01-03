### Код загрузки первичных данных для БД из parquet файла.

import logging
import dask.dataframe as dd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
import dask

# Указываем путь к проекту через переменную окружения
path = os.environ.get('PROJECT_PATH', '..')

# Конфигурируем логирование для Airflow
logger = logging.getLogger('airflow')
logger.setLevel(logging.DEBUG)  # Обновлено на DEBUG для более подробных логов

# Создание обработчика для записи в консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Для вывода всех сообщений, включая DEBUG
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


# Типы данных для ga_sessions_df
ga_sessions_columns_types = {
    'session_id': 'object',
    'client_id': 'object',
    'visit_number': 'int64',
    'utm_source': 'object',
    'utm_medium': 'object',
    'utm_campaign': 'object',
    'utm_adcontent': 'object',
    'utm_keyword': 'object',
    'device_category': 'object',
    'device_os': 'object',
    'device_brand': 'object',
    'device_model': 'object',
    'device_screen_resolution': 'object',
    'device_browser': 'object',
    'geo_country': 'object',
    'geo_city': 'object'
}
ga_sessions_date_columns = ['visit_date']
ga_sessions_time_columns = ['visit_time']

# Типы данных для ga_hits_df
ga_hits_columns_types = {
    'session_id': 'object',
    'hit_number': 'int64',
    'hit_type': 'object',
    'hit_referer': 'object',
    'hit_page_path': 'object',
    'event_category': 'object',
    'event_action': 'object',
    'event_label': 'object',
    'event_value': 'object'
}
ga_hits_date_columns = ['hit_date']
ga_hits_time_columns = ['hit_time']

# Функция для загрузки и обработки данных
def load_and_process_data(file_path: str, columns_types: dict, date_columns: list, time_columns: list) -> dd.DataFrame:
    try:
        logger.info(f"Загружаем файл: {file_path}")
        df = dd.read_parquet(file_path, chunksize='100MB')  # Уменьшаем размер чанков

        # Удаляем дубликаты
        df = df.drop_duplicates()

        # Преобразуем все данные в строки
        df = df.astype(str)

        # Заменяем пропуски на 'unknown'
        df = df.fillna("unknown")

        # Заменяем неинформативные значения
        df = df.replace(["(no set)", "(none)", "na"], "unknown")

        # Приводим столбцы к нужным типам данных
        for col, dtype in columns_types.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        # Преобразуем столбцы с датами
        for col in date_columns:
            if col in df.columns:
                df[col] = dd.to_datetime(df[col], errors='coerce')

        # Преобразуем столбцы с временем
        for col in time_columns:
            if col in df.columns:
                df[col] = dd.to_datetime(df[col], format='%H:%M:%S', errors='coerce')

        logger.info(f"Файл {file_path} успешно загружен и обработан.")
        return df
    except Exception as e:
        logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}")
        raise Exception(f"Ошибка при обработке файла {file_path}: {str(e)}")

# Функция для обработки выбросов
def handle_outliers(df: dd.DataFrame) -> dd.DataFrame:
    try:
        logger.info("Обрабатываем выбросы.")
        # Для каждой числовой колонки вычисляем IQR и фильтруем выбросы
        for col in df.select_dtypes(include=['int64', 'float64']).columns:
            q1 = df[col].quantile(0.25).compute()  # .compute() для вычисления значения
            q3 = df[col].quantile(0.75).compute()
            iqr = q3 - q1
            df = df[(df[col] >= (q1 - 1.5 * iqr)) & (df[col] <= (q3 + 1.5 * iqr))]

        logger.info("Выбросы успешно обработаны.")
        return df
    except Exception as e:
        logger.error(f"Ошибка при обработке выбросов: {str(e)}")
        raise Exception(f"Ошибка при обработке выбросов: {str(e)}")

# Функция для проверки наличия данных в базе данных
def check_if_data_exists(df: dd.DataFrame, table_name: str, engine) -> bool:
    try:
        logger.debug(f"Начинаем проверку данных в базе для таблицы {table_name}.")

        # Преобразуем Dask DataFrame в Pandas для выполнения запроса
        session_ids = df['session_id'].compute().unique()

        # Создаем подключение для выполнения запросов
        conn = engine.connect()

        # Логируем количество уникальных session_id для диагностики
        logger.debug(f"Найдено {len(session_ids)} уникальных session_id для проверки.")

        # Строим запрос для проверки данных по партиям
        chunk_size = 1000  # Размер каждой партии
        futures = []

        # Используем Dask для параллельной обработки запросов
        for i in range(0, len(session_ids), chunk_size):
            chunk = session_ids[i:i + chunk_size]
            futures.append(dask.delayed(execute_check)(conn, table_name, chunk))

        # Выполняем все задачи параллельно
        dask.compute(*futures)
        return True

    except Exception as e:
        logger.error(f"Ошибка при проверке данных в базе данных: {str(e)}")
        raise Exception(f"Ошибка при проверке данных в базе данных: {str(e)}")


def execute_check(conn, table_name, chunk):
    query = text(f"SELECT COUNT(*) FROM {table_name} WHERE session_id IN :session_ids")
    result = conn.execute(query, {"session_ids": tuple(chunk)}).fetchone()
    logger.debug(f"Обработано {len(chunk)} session_id. Результат: {result[0]} записей.")
    return result[0]

# Функция для загрузки данных в PostgreSQL
def load_to_postgresql(df: dd.DataFrame, table_name: str, db_url: str):
    try:
        logger.info(f"Загружаем данные в таблицу {table_name} в PostgreSQL.")
        engine = create_engine(db_url)

        # Проверка наличия данных в таблице
        if check_if_data_exists(df, table_name, engine):
            logger.info(f"Завершаем выполнение, так как все данные уже присутствуют в таблице {table_name}.")
            return

        # Загрузка данных в PostgreSQL
        df.compute().to_sql(table_name, engine, index=False, if_exists='append')  # Используем 'append' для добавления
        logger.info(f"Данные успешно загружены в таблицу {table_name}.")
    except SQLAlchemyError as e:
        logger.error(f"Ошибка при загрузке данных в PostgreSQL: {str(e)}")
        raise Exception(f"Ошибка при загрузке данных в PostgreSQL: {str(e)}")

# Основная ETL функция
def run_etl():
    # Загружаем и обрабатываем данные
    ga_sessions_df = load_and_process_data(
        f'{path}/data/ga/ga_sessions.parquet',
        ga_sessions_columns_types,
        ga_sessions_date_columns,
        ga_sessions_time_columns
    )
    ga_hits_df = load_and_process_data(
        f'{path}/data/ga/ga_hits.parquet',
        ga_hits_columns_types,
        ga_hits_date_columns,
        ga_hits_time_columns
    )

    # Обработка выбросов
    ga_sessions_df = handle_outliers(ga_sessions_df)
    ga_hits_df = handle_outliers(ga_hits_df)

    # Загрузка в PostgreSQL
    db_url = 'postgresql://airflow_db:airflow@192.168.0.106:5432/airflow_metadata?sslmode=disable'
    load_to_postgresql(ga_sessions_df, 'ga_sessions', db_url)
    load_to_postgresql(ga_hits_df, 'ga_hits', db_url)


if __name__ == "__main__":
    run_etl()
