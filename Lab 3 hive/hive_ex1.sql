USE belashovvi;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=116;
SET hive.exec.max.dynamic.partitions.pernode=116;

dROP TABLE IF EXISTS user_logs;
CREATE EXTERNAL TABLE user_logs (ip string, time_full string, request string, size int, status int, browser string) 
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES ('input.regex'='^(\\S+)\\t\\t\\t(\\d+)\\t(\\S+)\\t(\\d+)\\t(\\d+)\\t(.*)$')
--    WITH SERDEPROPERTIES ('input.regex'='^(\\S+)\\t\\t\\t(\\d+)\\t(\\S+)\\t(\\d+)\\t(\\d+)\\t(\\S+).*')
  LOCATION '/data/user_logs/user_logs_M'; 

DROP TABLE IF EXISTS logs;
--create table logs(ip string, time_full string, request string, size int, status int, browser string)
create table logs(ip string, request string, size int, status int, browser string)
partitioned by (time_date string);
insert into logs partition(time_date) select ip, request, size, status, browser, substr(time_full,0,8) from user_logs;

select * from logs limit 10;

DROP TABLE IF EXISTS users;
CREATE EXTERNAL TABLE users (ip string, browser string, gender string, age TINYINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M';

select * from users limit 10;


DROP TABLE IF EXISTS ipregions;
CREATE EXTERNAL TABLE ipregions (ip string, region string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M';

select * from ipregions limit 10;


DROP TABLE IF EXISTS subnets;
CREATE EXTERNAL TABLE subnets (ip string, subnet string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';

select * from subnets limit 10;


