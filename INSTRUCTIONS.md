# Запуск 2 лабы

1. Поднимаем докер:
```bash
docker-compose up -d
```
Скрипт сам накатит таблички и зальет данные из csv файлов.

2. Запускаем первый скрипт, чтобы собрать "звезду" в постгресе:
```bash
docker exec -it bigdataspark-spark-1 /opt/spark/bin/spark-submit \
  --jars /opt/spark/jobs/postgresql-42.6.0.jar \
  /opt/spark/jobs/lab2_postgres.py
```

3. Запускаем второй скрипт, чтобы собрать витрины в кликхаусе:
```bash
docker exec -it bigdataspark-spark-1 /opt/spark/bin/spark-submit \
  --jars /opt/spark/jobs/postgresql-42.6.0.jar,/opt/spark/jobs/clickhouse-jdbc-0.4.6-all.jar \
  /opt/spark/jobs/lab2_clickhouse.py
```

4. Проверка:
- Postgres торчит на порту `5432` (пользователь `user`, пароль `password`, бд `analytics`).
- ClickHouse торчит на порту `8123` (пользователь `default`, пароль `password`, бд `analytics`).
