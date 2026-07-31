FROM apache/airflow:2.10.5-python3.12

# Install project dependencies (no dbt/streamlit yet — those come in later milestones).
COPY pyproject.toml README.md /opt/airflow/project/
COPY src/ /opt/airflow/project/src/

# Editable install: docker-compose.yml mounts ./src over /opt/airflow/project/src,
# so this resolves to the mounted host directory and host edits take effect
# without rebuilding the image.
RUN pip install --no-cache-dir -e /opt/airflow/project
