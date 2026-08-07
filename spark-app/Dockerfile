# Optional image for running the Spark job locally (outside the compose stack).
# Within the Docker Compose stack, spark-master/spark-worker (bitnami/spark)
# mount ./spark-app and run the job via spark-submit instead.
FROM bitnamilegacy/spark:3.3

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_stream.py .

USER 1001

CMD ["/bin/bash"]