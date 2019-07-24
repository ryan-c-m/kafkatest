FROM python:3
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . /app/
WORKDIR /app/
#ENTRYPOINT ["python3", "-s", "-m", "pytest", "/app/tests"]
ENTRYPOINT ["python3", "/app/kafkatest/main.py"]
