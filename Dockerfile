FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./
COPY source_app_store_connect ./source_app_store_connect
COPY spec.json ./

ENV AIRBYTE_ENTRYPOINT="python /app/main.py"

ENTRYPOINT ["python", "/app/main.py"]
