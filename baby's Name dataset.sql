-- Databricks notebook source
-- MAGIC %md
-- MAGIC The example uses a publicly available dataset that contains records of New York State baby names. CSV data will be downloaded and saved to Databricks temp folder, then moved to the raw folder

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # IMPORTS
-- MAGIC import urllib
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC # File download
-- MAGIC dbutils.fs.mkdirs('dbfs:/downloads/data')
-- MAGIC urllib.request.urlretrieve('https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv', '/tmp/rows.csv')
-- MAGIC
-- MAGIC # Simulate Layers
-- MAGIC dbutils.fs.mkdirs('dbfs:/baby_names/raw')
-- MAGIC
-- MAGIC # Move to location
-- MAGIC dbutils.fs.mv("file:/tmp/rows.csv", "dbfs:/baby_names/raw/rows.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Validation of Raw folder Existance

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dbutils.fs.ls('dbfs:/baby_names/raw/'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Validate data loaded in Raw Folder

-- COMMAND ----------

SELECT * FROM csv.`dbfs:/baby_names/raw/rows.csv` limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC It is also possible to create table specific Schemas to process bronze, silver & gold dataset. However Schema creation is not supported in databrick's Community version

-- COMMAND ----------

-- CREATE SCHEMA IF NOT EXISTS samples.bronze;
-- CREATE SCHEMA IF NOT EXISTS samples.Silver;
-- CREATE SCHEMA IF NOT EXISTS samples.gold;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drop existing bronze Table

-- COMMAND ----------

DROP TABLE IF EXISTS baby_names_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create bronze table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS baby_names_bronze (
    Year INT,
    First_Name STRING,
    County STRING,
    Sex STRING,
    Count INT,
    _file_path STRING,
    _ingest_time TIMESTAMP
)
USING delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Popular baby first names in New York will be ingested from the New York State Department of Health into bronze layer

-- COMMAND ----------

COPY INTO baby_names_bronze
FROM (
    SELECT 
        CAST(Year AS INT) AS Year,
        `First Name` AS First_Name,  -- Enclose column names with spaces in backticks
        County,
        Sex,
        CAST(Count AS INT) AS Count,
        _metadata.file_path AS _file_path,  -- Correct reference to metadata
        current_timestamp() AS _ingest_time
    FROM 
        'dbfs:/baby_names/raw/rows.csv'  -- Remove backticks from file path
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC fetch data from bronze layer

-- COMMAND ----------

select * from baby_names_bronze limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drop Silver Table

-- COMMAND ----------

drop table baby_names_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Silver Table

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS baby_names_silver (
    Year_Of_Birth INT,
    First_Name STRING,
    Count INT
)
USING DELTA;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC New York popular baby first name data cleaned and prepared for analysis into the Silver Layer table
-- MAGIC

-- COMMAND ----------

INSERT INTO baby_names_silver (Year_Of_Birth, First_Name, Count)
SELECT 
    Year AS Year_Of_Birth,
    First_Name AS First_Name,
    Count
FROM 
    baby_names_bronze -- Replace LIVE.baby_names_raw with baby_names_bronze
WHERE 
    "First Name" IS NOT NULL AND Count > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC fetch data from silver layer

-- COMMAND ----------

select * from baby_names_silver limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drop existing Gold  Table

-- COMMAND ----------

drop table baby_names_gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the Gold Layer table (if it doesn't exist)

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS baby_names_gold(
    First_Name STRING,
    Year_Of_Birth  INT,
    Total_Count INT
)
USING DELTA;




-- COMMAND ----------

-- MAGIC %md
-- MAGIC Insert New York popular baby first name data into the Gold Layer table

-- COMMAND ----------


INSERT INTO baby_names_gold (First_Name, Year_Of_Birth, Total_Count)
SELECT 
    First_Name,
    Year_Of_Birth,
    SUM(Count) AS Total_Count
FROM 
    baby_names_silver

GROUP BY 
    First_Name,Year_Of_Birth



-- COMMAND ----------

-- MAGIC %md
-- MAGIC fetch data from gold table

-- COMMAND ----------

select * from  baby_names_gold


-- COMMAND ----------

Summarizing counts of the top baby names for New York for 2021.

-- COMMAND ----------


SELECT 
    First_Name,
    Year_Of_Birth,
     Total_Count
FROM 
    baby_names_gold
WHERE 
    Year_Of_Birth = 2021
ORDER BY 
    Total_Count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The above option can be also achieved by creating Materialized view and same could be refreshed as part of pipeline.However, materialized view  and pipeline creation is not supported in community edition

-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS baby_names_gold_mv
AS
SELECT 
    First_Name,
    Year_Of_Birth,
     Total_Count
FROM 
    baby_names_gold
WHERE 
    Year_Of_Birth = 2021
ORDER BY 
    Total_Count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Refresh Materialized View with new dataset

-- COMMAND ----------

REFRESH MATERIALIZED VIEW baby_names_gold_mv;
