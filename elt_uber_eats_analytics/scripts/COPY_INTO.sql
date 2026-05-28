--USO DO COMANDO COPY INTO PARA INGESTAO DE DADOS
--https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into

CREATE TABLE IF NOT EXISTS uber.ingestion.kafka_status;

COPY INTO uber.ingestion.kafka_status
FROM '/Volumes/uber/ingestion/kafka'
FILEFORMAT = json
FORMAT_OPTIONS ('inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')