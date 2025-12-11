-- Simple sanity checks for the Hive MVP stack (no YARN required).
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SHOW DATABASES;
SELECT 1 as ok;
CREATE DATABASE IF NOT EXISTS tmp_smoke COMMENT \"temporary database for smoke testing\";
USE tmp_smoke;
CREATE EXTERNAL TABLE IF NOT EXISTS pong_meta_only (value INT) LOCATION \"hdfs:///tmp/hive-pong\";
DROP TABLE pong_meta_only;
DROP DATABASE tmp_smoke;
