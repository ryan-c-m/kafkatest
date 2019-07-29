FROM python:3
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY kafkatest /app/kafkatest
COPY tests-unit /app/tests-unit
WORKDIR /app/
ENTRYPOINT ["python3", "/app/kafkatest/main.py"]
