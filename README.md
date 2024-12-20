# ETL Process for Google Analytics Data

## 📌 Описание проекта
Проект предназначен для автоматизации обработки и загрузки данных Google Analytics (GA) в PostgreSQL. Используемые технологии:
- **Python** и **Dask** для обработки больших данных.
- **Apache Airflow** для автоматизации.
- **PostgreSQL** для хранения данных.

## 🛠️ Основной функционал
1. **Загрузка данных**:
   - Форматы: Parquet и JSON.
   - Удаление дубликатов, обработка пропусков и выбросов.
   - Преобразование данных: типы данных, временные метки.
2. **Обработка выбросов**:
   - Применение метода межквартильного размаха (IQR).
3. **Загрузка данных в PostgreSQL**:
   - Уникальные идентификаторы для предотвращения дубликатов.
4. **Автоматизация с Airflow**:
   - DAG для управления процессами ETL.

## 📂 Структура проекта
- `sberauto_dag.py`: DAG для Apache Airflow.
- `parquet_processing.py`: Скрипт обработки данных Parquet.
- `json_processing.py`: Скрипт обработки данных JSON.
- `db_setup.py`: Скрипт для создания базы данных и таблиц.
- `requirements.txt`: Список используемых зависимостей.

## 🧩 Установка зависимостей
1. Убедитесь, что установлен Python 3.x и виртуальное окружение активировано.
2. Создайте и активируйте виртуальное окружение:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Linux/MacOS
   .venv\Scripts\activate      # Windows
3. Установите зависимости из файла `requirements.txt`:
   ```bash
   pip install -r requirements.txt

🚀 **Как запустить проект**
===========================

1. **Настройка базы данных**:
   * Убедитесь, что PostgreSQL установлен и запущен.
   * Запустите скрипт для создания базы данных и таблиц:

   ```bash
   python db_setup.py

2. **Укажите путь к проекту**:
   * Добавьте переменную окружения для указания пути к проекту:

   ```python
   os.environ['PROJECT_PATH'] = '/path/to/your/project'

3. **Настройка подключения к базе данных**:
   * Укажите строку подключения в скриптах обработки:

   ```python
   db_url = 'postgresql://airflow_db:airflow@192.168.0.106:5432/airflow_metadata?sslmode=disable'

4. **Запуск Airflow**

    - Инициализируйте базу данных Airflow:

    ```bash
    airflow db init
    ```

    - Запустите веб-сервер и планировщик:

    ```bash
    airflow webserver
    airflow scheduler
    ```

    - Откройте интерфейс Airflow по адресу [http://localhost:8080](http://localhost:8080) и активируйте DAG.


5. **Логирование**

   Все действия логируются с использованием стандартной библиотеки `logging`. Логи выводятся в консоль и могут быть полезны для отслеживания ошибок и выполнения задач.


6. **Важные замечания**

   - Убедитесь, что PostgreSQL настроен и доступен для подключения.
   - Проект разработан для работы с большими объемами данных и использует возможности Dask для масштабируемости.
   - Перед запуском проверьте настройки путей и переменных окружения.
