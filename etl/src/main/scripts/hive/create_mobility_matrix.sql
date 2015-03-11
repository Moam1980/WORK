CREATE DATABASE IF NOT EXISTS ops;

USE ops;

DROP TABLE IF EXISTS mobility_matrix_spark_ext;
CREATE EXTERNAL TABLE mobility_matrix_spark_ext(
  start_interval_init_time string,
  end_interval_init_time string,
  start_location string,
  end_location string,
  num_weeks int,
  imei string,
  imsi string,
  msisdn string,
  weight double)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/user/tdatuser/clients/ada/mobility-matrix/0.8/2014/11/csv/';
