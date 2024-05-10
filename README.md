# ETL_Weather

1. Создать папку airflow и переместить в неё файлы репозитория 
2. Создать папки logs и plugins
3. Добавить в docker-compose.yaml 
  database:
    image: postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: rainbow_database
    ports:
      - "5423:5432"
4. В консоли ввести команду "docker compose up"
5. Добавить ключ API через GUI Airflow
6. Добавить подключение ко второй БД через GUI Airflow
