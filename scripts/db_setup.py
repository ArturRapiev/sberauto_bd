import psycopg2
from psycopg2 import sql

# Константы подключения
DB_HOST = "192.168.0.106"
DB_PORT = 5432
DB_USER = "postgres"  # Это суперпользователь PostgreSQL
DB_PASSWORD = "postgres"  # Пароль, который вы установили для пользователя postgres
DB_NAME = "airflow_metadata"

# SQL для создания таблиц
CREATE_SESSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS ga_sessions (
    session_id VARCHAR(255) PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    visit_date DATE NOT NULL,
    visit_time TIME NOT NULL,
    visit_number INTEGER NOT NULL,
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255),
    utm_campaign VARCHAR(255),
    utm_keyword VARCHAR(255),
    device_category VARCHAR(255),
    device_os VARCHAR(255),
    device_brand VARCHAR(255),
    device_model VARCHAR(255),
    device_screen_resolution VARCHAR(255),
    device_browser VARCHAR(255),
    geo_country VARCHAR(255),
    geo_city VARCHAR(255)
);
"""

CREATE_HITS_TABLE = """
CREATE TABLE IF NOT EXISTS ga_hits (
    hit_id SERIAL PRIMARY KEY,
    session_id VARCHAR(255) REFERENCES ga_sessions(session_id) ON DELETE CASCADE,
    hit_date DATE NOT NULL,
    hit_time TIME NOT NULL,
    hit_number INTEGER NOT NULL,
    hit_type VARCHAR(255),
    hit_referer VARCHAR(255),
    hit_page_path TEXT,
    event_category VARCHAR(255),
    event_action VARCHAR(255),
    event_label VARCHAR(255),
    event_value VARCHAR(255),
    CONSTRAINT unique_hit UNIQUE(session_id, hit_number, hit_type)
);
"""
def check_table_exists(cursor, table_name):
    """Функция для проверки, существует ли таблица"""
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '{table_name}'
        );
    """)
    return cursor.fetchone()[0]

# Подключение как суперпользователь
def setup_database():
    try:
        # Подключение как суперпользователь
        admin_conn = psycopg2.connect(
            dbname="postgres",
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        admin_conn.autocommit = True
        admin_cursor = admin_conn.cursor()

        # Создание пользователя airflow_db
        try:
            admin_cursor.execute("CREATE USER airflow_db WITH PASSWORD 'airflow';")
            print("Пользователь 'airflow_db' успешно создан.")
        except psycopg2.errors.DuplicateObject:
            print("Пользователь 'airflow_db' уже существует.")

        # Создание базы данных
        try:
            admin_cursor.execute(f"CREATE DATABASE {DB_NAME} OWNER airflow_db;")
            print(f"База данных '{DB_NAME}' успешно создана.")
        except psycopg2.errors.DuplicateDatabase:
            print(f"База данных '{DB_NAME}' уже существует.")

        admin_cursor.close()
        admin_conn.close()

    except Exception as e:
        print(f"Ошибка подключения администратора: {e}")
        return

# Подключение как пользователь airflow_db
    try:
        user_conn = psycopg2.connect(
            dbname=DB_NAME,
            user="airflow_db",
            password="airflow",
            host=DB_HOST,
            port=DB_PORT
        )
        user_cursor = user_conn.cursor()

        # Проверка и создание таблиц, если их нет
        if not check_table_exists(user_cursor, "ga_sessions"):
            user_cursor.execute(CREATE_SESSIONS_TABLE)
            print("Таблица 'ga_sessions' успешно создана.")
        else:
            print("Таблица 'ga_sessions' уже существует.")

        if not check_table_exists(user_cursor, "ga_hits"):
            user_cursor.execute(CREATE_HITS_TABLE)
            print("Таблица 'ga_hits' успешно создана.")
        else:
            print("Таблица 'ga_hits' уже существует.")

        user_conn.commit()
        user_cursor.close()
        user_conn.close()

    except Exception as e:
        print(f"Ошибка подключения пользователя: {e}")

if __name__ == "__main__":
    setup_database()
