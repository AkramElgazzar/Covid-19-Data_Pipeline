-- Configure Hive for dynamic partitioning with appropriate settings.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 100000;
SET hive.exec.max.dynamic.partitions.pernode = 100000;


-- Create a database named 'covid_db' to store COVID-19 data.
CREATE DATABASE IF NOT EXISTS covid_db;


-- Create an external table named 'covid_staging' in the 'covid_db' database.
-- This table stores the raw COVID-19 data in CSV format.
CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_staging (
  Country STRING,
  Total_Cases DOUBLE,
  New_Cases DOUBLE,
  Total_Deaths DOUBLE,
  New_Deaths DOUBLE,
  Total_Recovered DOUBLE,
  Active_Cases DOUBLE,
  Serious DOUBLE,
  Tot_Cases DOUBLE,
  Deaths DOUBLE,
  Total_Tests DOUBLE,
  Tests DOUBLE,
  CASES_per_Test DOUBLE,
  Death_in_Closed_Cases STRING,
  Rank_by_Testing_rate DOUBLE,
  Rank_by_Death_rate DOUBLE,
  Rank_by_Cases_rate DOUBLE,
  Rank_by_Death_of_Closed_Cases DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
TBLPROPERTIES ("skip.header.line.count" = "1");


-- Create an external partitioned table named 'covid_ds_partitioned' in the 'covid_db' database.
-- This table will store processed data partitioned by country for efficient retrieval.
CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_ds_partitioned (
  Country STRING,
  Total_Cases STRING,
  New_Cases STRING,
  Total_Deaths STRING,
  New_Deaths STRING,
  Total_Recovered STRING,
  Active_Cases STRING,
  Serious STRING,
  Tot_Cases STRING,
  Deaths STRING,
  Total_Tests STRING,
  Tests STRING,
  CASES_per_Test STRING,
  Death_in_Closed_Cases STRING,
  Rank_by_Testing_rate STRING,
  Rank_by_Death_rate STRING,
  Rank_by_Cases_rate STRING,
  Rank_by_Death_of_Closed_Cases STRING
)
PARTITIONED BY (COUNTRY_NAME STRING)
LOCATION '/user/cloudera/ds/partitioned';


-- Insert data into the 'covid_ds_partitioned' table from the 'covid_staging' table,
-- excluding rows where 'Country' is "World" and handling potentially empty values.
INSERT OVERWRITE TABLE covid_db.covid_ds_partitioned PARTITION (COUNTRY_NAME)
SELECT Country,
       COALESCE(Total_Cases, '0') AS Total_Cases, -- Handle empty values with '0'
       COALESCE(New_Cases, '0') AS New_Cases,
       COALESCE(Total_Deaths, '0') AS Total_Deaths,
       New_Deaths,
       Total_Recovered,
       Active_Cases,
       Serious,
       Tot_Cases,
       Deaths,
       COALESCE(Total_Tests, '0') AS Total_Tests, -- Handle empty values with '0'
       Tests,
       CASES_per_Test,
       Death_in_Closed_Cases,
       Rank_by_Testing_rate,
       Rank_by_Death_rate,
       Rank_by_Cases_rate,
       Rank_by_Death_of_Closed_Cases,
       Country
FROM covid_db.covid_staging
WHERE Country != 'World';



