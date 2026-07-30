FROM apache/airflow:2.10.5-python3.12

# Install project dependencies (no dbt/streamlit yet — those come in later milestones).
COPY pyproject.toml README.md /opt/airflow/project/
COPY src/ /opt/airflow/project/src/

RUN pip install --no-cache-dir /opt/airflow/project
