
FROM apache/airflow:2.10.5-python3.11

USER root

# Docker CLI pour permettre aux tâches Airflow
# d'exécuter docker exec et docker cp.
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Dépendances Python du projet et du backend PostgreSQL.
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    pandas==2.2.3 \
    psycopg2-binary==2.9.10

# Identifiant de build, utilisé pour renouveler le compte admin.
ARG AIRFLOW_BUILD_ID=dev
ENV AIRFLOW_BUILD_ID=${AIRFLOW_BUILD_ID}

COPY --chown=airflow:root start-airflow.sh /opt/airflow/start-airflow.sh

USER root
RUN chmod +x /opt/airflow/start-airflow.sh
USER airflow

CMD ["bash", "/opt/airflow/start-airflow.sh"]
