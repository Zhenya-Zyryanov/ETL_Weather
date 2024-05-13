# ETL_Weather

1. Установить Docker
2. Создать папку airflow и переместить в неё файлы репозитория
3. Создать папки logs и plugins
4. Добавить в docker-compose.yaml 
  database:
    image: postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: rainbow_database
    ports:
      - "5423:5432"
5. В консоли ввести команду "docker compose up"
6. Добавить ключ API через GUI Airflow
7. Добавить подключение ко второй БД через GUI Airflow
