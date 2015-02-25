CREATE DATABASE IF NOT EXISTS ops;

USE ops;

DROP TABLE IF EXISTS metrics_hadoop_files_ext;
CREATE EXTERNAL TABLE metrics_hadoop_files_ext(
  source_name string,
  source_format string,
  file_name string,
  date_file string,
  modification_date string,
  access_date string,
  size_bytes bigint)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/user/tdatuser/metrics/hadoop-files/csv/';
