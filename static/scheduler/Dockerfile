FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY staticScheduler.py ./
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "./staticScheduler.py"]
