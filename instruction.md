1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/tng00/BigDataSpark.git
    ```

2. Перейдите в каталог проекта:

   ```bash
   cd BigDataSpark
   ```
3. Поднимите все сервисы:

   ```bash
   docker compose up
   ```
4. Загрузите начальные данные в PostgreSQL:

   ```bash
   docker exec -it bigdataspark-postgres-1 \
     psql -U spark_user -d spark_db \
     -f /import_data/sql/init_mock_data.sql
   ```
5. Примените DDL-схему в PostgreSQL:

   ```bash
   docker exec -it bigdataspark-postgres-1 \
     psql -U spark_user -d spark_db \
     -f /import_data/sql/ddl.sql
   ```
6. Запустите ETL-скрипт Spark для загрузки данных в PostgreSQL:

   ```bash
   docker exec -it spark-master \
     spark-submit \
       --master spark://spark-master:7077 \
       --deploy-mode client \
       --jars /opt/spark/jars/postgresql-42.6.0.jar \
       /opt/spark-apps/spark_etl.py
   ```
7. Запустите скрипт Spark для выгрузки данных из PostgreSQL в ClickHouse:

   ```bash
   docker exec -it spark-master \
     spark-submit \
       --master spark://spark-master:7077 \
       --deploy-mode client \
       --jars "/opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/clickhouse-jdbc-0.4.6.jar" \
       --driver-class-path "/opt/spark/jars/postgresql-42.6.0.jar:/opt/spark/jars/clickhouse-jdbc-0.4.6.jar" \
       /opt/spark-apps/clickhouse.py
   ```
8. Проверьте список таблиц в ClickHouse:

   ```bash
   docker exec -it bigdataspark-clickhouse-1 \
     clickhouse-client --query "SHOW TABLES;"
   ```

