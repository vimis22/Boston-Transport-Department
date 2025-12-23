CREATE DATABASE IF NOT EXISTS raw LOCATION 'hdfs:///warehouse/raw';
CREATE DATABASE IF NOT EXISTS enriched LOCATION 'hdfs:///warehouse/enriched';
CREATE DATABASE IF NOT EXISTS model LOCATION 'hdfs:///warehouse/model';
CREATE DATABASE IF NOT EXISTS errors LOCATION 'hdfs:///warehouse/errors';
SHOW DATABASES;
