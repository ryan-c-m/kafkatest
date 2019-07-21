FROM python:3
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . /app/
#ENTRYPOINT ["python3", "/app/kafka-test/main.py"]
WORKDIR /app/
RUN pylint kafkatest
ENTRYPOINT ["python3", "-m", "pytest", "/app/tests"]
