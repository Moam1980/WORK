CREATE DATABASE IF NOT EXISTS ops;

USE ops;

DROP TABLE IF EXISTS subscribers_profiling_ext;
CREATE EXTERNAL TABLE subscribers_profiling_ext(
  affluence string,
  age string,
  imsi string,
  nationality string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/user/tdatuser/clients/ada/subscribers/profiling/0.8/csv/';
