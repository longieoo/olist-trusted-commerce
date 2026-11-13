FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu
USER root
WORKDIR /app
COPY pyproject.toml /app/pyproject.toml
COPY requirements.lock /app/requirements.lock
COPY src /app/src
RUN python3 -m pip install --no-cache-dir pip==25.0.1 setuptools==75.8.2 wheel==0.45.1 && python3 -m pip install --no-cache-dir -c requirements.lock '.[test,warehouse]' && python3 -c 'import psycopg, boto3, pyarrow, pytest'
# Resolve the Kafka connector and S3A matching Spark's Hadoop 3.3.4 distribution.
RUN mkdir -p /opt/spark/extra-jars && python3 - <<'PY'
import urllib.request
base='https://repo.maven.apache.org/maven2/'
artifacts=[('org/apache/hadoop','hadoop-aws','3.3.4'),('com/amazonaws','aws-java-sdk-bundle','1.12.262'),('org/apache/spark','spark-sql-kafka-0-10_2.12','3.5.8'),('org/apache/spark','spark-token-provider-kafka-0-10_2.12','3.5.8'),('org/apache/kafka','kafka-clients','3.4.1'),('org/apache/commons','commons-pool2','2.11.1')]
for group,name,version in artifacts:
    filename=f'{name}-{version}.jar'
    urllib.request.urlretrieve(f'{base}{group}/{name}/{version}/{filename}',f'/opt/spark/extra-jars/{filename}')
PY
ENV PYTHONPATH=/app/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip SPARK_LOCAL_IP=127.0.0.1
RUN python3 -m pip install --no-cache-dir duckdb==1.2.2 streamlit==1.44.1
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["python3", "-m", "olist.cli"]
