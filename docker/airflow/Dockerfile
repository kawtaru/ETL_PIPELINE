FROM apache/airflow:2.10.0

# (Optional) ODBC driver + tools so pyodbc works inside Airflow tasks
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl apt-transport-https gnupg2 \
 && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
 && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 mssql-tools18 unixodbc-dev \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV PATH="$PATH:/opt/mssql-tools18/bin"
USER airflow

# Airflow provider to talk to SQL Server from tasks (not for Airflow metadata!)
RUN pip install --no-cache-dir \
    apache-airflow-providers-microsoft-mssql==3.7.1 \
    pymssql==2.2.11 \
    pyodbc==5.1.0
