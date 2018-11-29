FROM jaeyeun97/spark

COPY . .

ENTRYPOINT [ "/opt/entrypoint.sh", "/opt/spark/bin/spark-class", "org.apache.spark.deploy.ExternalShuffleService", "1" ]
