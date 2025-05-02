#FROM apache/airflow:2.10.5
FROM apache/airflow:2.10.2
COPY requirements.txt /
#RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt