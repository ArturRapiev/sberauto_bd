### Новый файл для настройки базы данных: `db_setup.py`

import psycopg2
from psycopg2 import sql

# Конфигурация подключения
DB_URL = "postgresql://airflow_db:airflow@192.168.0.106:5432/airflow_metadata"

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

def setup_database():
    try:
        with psycopg2.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                # Создаем таблицы
                cur.execute(CREATE_SESSIONS_TABLE)
                cur.execute(CREATE_HITS_TABLE)
                print("Таблицы успешно созданы.")
    except Exception as e:
        print(f"Ошибка при настройке базы данных: {e}")

if __name__ == "__main__":
    setup_database()
