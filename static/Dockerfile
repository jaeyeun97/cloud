FROM ubuntu:16.04
COPY staticScheduler.py ./
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /
ENTRYPOINT ["python"]
CMD ["python", "./staticScheduler.py"]
