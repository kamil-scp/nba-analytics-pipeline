FROM python:3.11-slim

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONUNBUFFERED=1
ENV PATH="$AIRFLOW_HOME/.local/bin:$PATH"
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/keys/service_account.json

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p $AIRFLOW_HOME/dags \
             $AIRFLOW_HOME/dbt \
             $AIRFLOW_HOME/keys

COPY dags/ $AIRFLOW_HOME/dags/
COPY dbt/ $AIRFLOW_HOME/dbt/
COPY keys/ $AIRFLOW_HOME/keys/

# Airflow 3.0: standalone mode zamiast webserver/scheduler
CMD ["bash", "-c", "airflow db migrate && exec airflow standalone"]
