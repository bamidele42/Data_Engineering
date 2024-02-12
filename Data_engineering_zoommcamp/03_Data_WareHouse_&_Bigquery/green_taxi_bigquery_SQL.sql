-- creating external table referring to gcs
CREATE OR REPLACE EXTERNAL TABLE `sunny-web-411214.ny_taxi.external_green_tripdata`
OPTIONS(
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-temitope-bamidele/green/green_tripdata_2022-*.parquet']
);

SELECT * FROM `ny_taxi.external_green_tripdata`;

--How many records have a fare_amount of 0?
SELECT * FROM  `ny_taxi.external_green_tripdata`
  WHERE fare_amount = 0;

  --Creating a non partitioned table from external table
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_non_partitioned AS
SELECT * FROM ny_taxi.external_green_tripdata;

--Creating a partitioned table from external table
CREATE OR REPLACE TABLE `ny_taxi.green_tripdata_partitioned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `ny_taxi.external_green_tripdata`;

--Impact of partitioning
SELECT * FROM `ny_taxi.green_tripdata_non_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
ORDER BY PUlocationID ASC;

SELECT * FROM `ny_taxi.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
ORDER BY PUlocationID ASC;

--Creating a partition and cluster table
CREATE OR REPLACE TABLE `ny_taxi.green_tripdata_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `ny_taxi.external_green_tripdata`;

--Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
SELECT DISTINCT(PUlocationID) FROM `ny_taxi.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';