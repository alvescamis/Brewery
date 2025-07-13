# Brewery

#Instruções para rodar o projeto local

# Suba o ambiente
docker-compose up -d

# Copie os arquivos para dentro do container do spark-master
docker cp spark/ spark-master:/opt/spark/jobs/

# Execute manualmente a transformação:
docker exec -it spark-master spark-submit /opt/spark/jobs/transform.py 2025-07-12

# Segue a estrutura do projeto.

.
├── dags/
│   └── ingest_breweries.py         ← Airflow DAG
├── spark/
│   ├── ingestion.py                ← Grava JSON no GCS (Bronze)
│   ├── transform.py                ← PySpark: JSON → Parquet por estado (Silver)
│   ├── aggregate.py                ← PySpark: Agregação por tipo e estado (Gold)
│   └── tests/
│       └── test_ingestion.py       ← Testes unitários com mocks
├── Dockerfile                      ← Ambiente Python/Spark
└── docker-compose.yml             ← Airflow + Spark + GCS local (MinIO opcional)

