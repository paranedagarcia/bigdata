# Big Data

Manejo de registros de alto volumen basado en base de datos NoSQL (MongoDB)
## Framework FastAPI

### Problema
Traspasar 100 bases de datos en MySQL con 250 tablas y un total actualizado de 140 millones de registros hacia una nueva plataforma de bases de datos.


### PostgreSQL

### Azure Cosmos DB

### MongoDB Atlas


### Apache Airflow

```
export AIRFLOW_HOME=~/airflow
AIRFLOW_VERSION=3.1.3

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 3.0.0 with python 3.10: https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.10.txt

uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```
