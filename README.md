# Class: Big Data

## Getting started


- `docker compose up -d`
This command start mongo, mongo-express and kafka

Start metabase in seperate docker
- `docker pull metabase/metabase:latest`

- `docker run -d -p 3000:3000 --name metabase metabase/metabase`

Download airflow https://airflow.apache.org/docs/apache-airflow/stable/start.html

locate to airflow folder, create dags file, and copy airflow/ example file to that dags

- `airflow standalone`





Running
1. ETL meter-producer.py 
2. ETL spark-mongo.py 
3. ETL data-mongo.py
4. /Users/tranan/Desktop/dev/spark/spark-3.5.1-bin-hadoop3/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --master local\[4\] \
  Streaming.py