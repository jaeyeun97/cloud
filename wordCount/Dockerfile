FROM jaeyeun97/spark-py:latest

COPY jars/aws-java-sdk-1.11.450.jar /opt/spark/jars
COPY jars/hadoop-aws-3.1.1.jar /opt/spark/jars

COPY data /opt/spark/work-dir/data
COPY step2.py /opt/spark/work-dir

ENTRYPOINT ["/opt/entrypoint.sh"]
