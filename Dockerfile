FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY . .
RUN pip install -e .
RUN pip install dbt-postgres
RUN cd dbt && dbt deps --profiles-dir .