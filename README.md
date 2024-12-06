ETL Process for Google Analytics Data

Описание:
Этот проект предназначен для обработки и загрузки данных из Google Analytics (GA) в базу данных PostgreSQL с использованием Python, Dask и Apache Airflow для автоматизации и масштабируемости. Процесс включает в себя несколько этапов:

Загрузка и обработка данных:
Загружаются данные в формате Parquet и JSON, содержащие информацию о сессиях и хитах пользователей.
Применяются преобразования данных: удаление дубликатов, обработка пропусков, преобразование типов данных и времени.
Для обработки выбросов используется метод межквартильного размаха (IQR), который фильтрует аномальные значения.

Загрузка данных в PostgreSQL:
После обработки данные загружаются в PostgreSQL, где они могут быть использованы для дальнейшего анализа.
Для проверки наличия данных в базе используется механизм запросов по уникальным идентификаторам сессий.

Автоматизация с использованием Apache Airflow:
Процесс обработки данных автоматизируется с помощью DAG (Directed Acyclic Graph) в Apache Airflow.
Каждый DAG выполняет последовательность задач для обработки данных и их загрузки в базу.

Структура проекта:
de_dag.py: Скрипт для создания и запуска DAG в Airflow, который управляет процессом обработки данных и их загрузки.
parquet_processing.py: Скрипт для обработки данных в формате Parquet, включая загрузку данных, преобразование типов, обработку выбросов и загрузку в PostgreSQL.
json_processing.py: Скрипт для обработки данных в формате JSON с применением тех же шагов, что и для Parquet.

Зависимости:
Python 3.x
Apache Airflow
Dask
pandas
sqlalchemy
psycopg2
JSON (для обработки данных в формате JSON)
PostgreSQL

Убедитесь, что все зависимости установлены.

Конфигурация:
Параметры подключения к базе данных PostgreSQL задаются в скрипте parquet_processing.py и json_processing.py в переменной db_url.

Пример:
python
db_url = 'postgresql://airflow_db:airflow@192.168.0.106:5432/airflow_metadata?sslmode=disable'

Путь к данным и проекту задается через переменные окружения:
python
os.environ['PROJECT_PATH'] = '/path/to/your/project'
Логирование
Все действия в процессе обработки и загрузки данных логируются с помощью стандартной библиотеки logging. Логи выводятся в консоль и могут быть полезны для отслеживания ошибок и прогресса выполнения задач.

Важные примечания
Убедитесь, что ваш PostgreSQL сервер настроен и доступен для подключения.
Весь процесс обработки данных является параллельным, используя Dask для эффективной работы с большими объемами данных.
Этот проект предназначен для работы с большими объемами данных и обеспечивает масштабируемость обработки через Dask, а также автоматизацию с помощью Airflow.

Работа с PostgreSQL:

1. Подключаемся к серверу PostgreSQL от имени суперпользователя (postgres):
bash
psql -U postgres

Создаем нового пользователя с паролем:
sql
CREATE USER airflow_db WITH PASSWORD 'airflow';

Создаем базу данных для хранения данных Google Analytics:
sql
CREATE DATABASE airflow_metadata;

Назначаем созданного пользователя владельцем базы данных:
sql
ALTER DATABASE airflow_metadata OWNER TO airflow_db;

Даем пользователю права на выполнение всех операций с базой данных:
sql
GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO airflow_db;

Даем права на схему public, чтобы пользователь мог создавать и изменять таблицы:
sql
GRANT ALL ON SCHEMA public TO airflow_db;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow_db;

Теперь пользователь airflow_db имеет доступ к базе данных airflow_metadata и может выполнять операции с таблицами.

2. Создаем таблицы ga_sessions и ga_hits. Добавляем уникальные ограничения для таблиц, чтобы предотвратить загрузку дубликатов.

Таблица ga_sessions:
sql
CREATE TABLE IF NOT EXISTS ga_sessions (
    session_id VARCHAR(255) PRIMARY KEY,                -- ID визита (уникальный)
    client_id VARCHAR(255) NOT NULL,                    -- ID посетителя
    visit_date DATE NOT NULL,                           -- Дата визита
    visit_time TIME NOT NULL,                           -- Время визита
    visit_number INTEGER NOT NULL,                      -- Порядковый номер визита клиента
    utm_source VARCHAR(255),                            -- Канал привлечения
    utm_medium VARCHAR(255),                            -- Тип привлечения
    utm_campaign VARCHAR(255),                          -- Рекламная кампания
    utm_keyword VARCHAR(255),                           -- Ключевое слово
    device_category VARCHAR(255),                       -- Тип устройства
    device_os VARCHAR(255),                             -- ОС устройства
    device_brand VARCHAR(255),                          -- Бренд устройства
    device_model VARCHAR(255),                          -- Модель устройства
    device_screen_resolution VARCHAR(255),              -- Разрешение экрана
    device_browser VARCHAR(255),                        -- Браузер
    geo_country VARCHAR(255),                           -- Страна
    geo_city VARCHAR(255)                               -- Город                              
);
Таблица ga_hits:
sql
CREATE TABLE IF NOT EXISTS ga_hits (
    hit_id SERIAL PRIMARY KEY,                          -- Уникальный идентификатор события
    session_id VARCHAR(255) REFERENCES ga_sessions(session_id) ON DELETE CASCADE, -- ID визита
    hit_date DATE NOT NULL,                             -- Дата события
    hit_time TIME NOT NULL,                             -- Время события
    hit_number INTEGER NOT NULL,                        -- Порядковый номер события в рамках сессии
    hit_type VARCHAR(255),                              -- Тип события
    hit_referer VARCHAR(255),                           -- Источник события
    hit_page_path TEXT,                                 -- Путь страницы
    event_category VARCHAR(255),                        -- Категория события
    event_action VARCHAR(255),                          -- Действие события
    event_label VARCHAR(255),                           -- Метка действия
    event_value VARCHAR(255),                           
    CONSTRAINT unique_hit UNIQUE(session_id, hit_number, hit_type) -- Уникальность события в рамках сессии
);

Далее, если необходимо, настройте IP и прием подключения в файлах конфигурации postgreSQL:
"pg_hba.conf" (доступ к базе данных)
"postgresql.conf" (настройка подключения).
