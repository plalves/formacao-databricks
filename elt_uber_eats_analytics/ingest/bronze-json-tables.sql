CREATE OR REFRESH STREAMING TABLE bronze_orders
AS 
SELECT * 
FROM STREAM read_files('/Volumes/uber/analytics/files/kafka_orders_*', format => 'json');

CREATE OR REFRESH STREAMING TABLE  bronze_status
AS 
SELECT * 
FROM STREAM read_files('/Volumes/uber/analytics/files/kafka_status_*', format => 'json');