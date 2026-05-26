CREATE OR REFRESH STREAMING LIVE TABLE silver_deliveries
AS
SELECT
  o.order_id,
  CAST(o.order_date AS timestamp) as order_date,
  o.restaurant_key,
  CAST(o.total_amount AS FLOAT) as total_amount,
  s.status.status_name as status_name
FROM STREAM(LIVE.bronze_orders) o
JOIN STREAM(LIVE.bronze_status) s
on o.order_id = s.order_identifier;