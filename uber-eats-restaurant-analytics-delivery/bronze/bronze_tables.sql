CREATE OR REFRESH STREAMING LIVE TABLE bronze_restaurants
COMMENT "Raw restaurant data from MySQL"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/uber/restaurants/files/restaurants/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);

CREATE OR REFRESH STREAMING LIVE TABLE bronze_ratings
COMMENT "Raw rating data from MySQL"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/uber/restaurants/files/ratings/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);

CREATE OR REFRESH STREAMING LIVE TABLE bronze_products
COMMENT "Raw product data from MySQL"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/uber/restaurants/files/products/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);

CREATE OR REFRESH STREAMING LIVE TABLE bronze_inventory
COMMENT "Raw inventory data from PostgreSQL"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/uber/restaurants/files/inventory/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);
