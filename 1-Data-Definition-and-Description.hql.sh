# Connect to hadoop using ssh (If required)
ssh $USER@20.245.236.124

cd
wget https://sadatashareagsparkml.blob.core.windows.net/hadoop-bangalore/hive_employee_data.zip
unzip -n hive_employee_data.zip
ls -al hive_employee_data
#hadoop fs -rmr hive_employee_data
hadoop fs -put hive_employee_data/
hadoop fs -ls /user/$USER/hive_employee_data
hadoop fs -ls /user/$USER/hive_employee_data/employee.txt

hive -e "CREATE DATABASE IF NOT EXISTS db_$USER"

hive

#################################-- - Note: Change below command to specify your database name
USE db_u20;

SELECT current_database();

SHOW TABLES;


- Hive Data Types

--Create table using ARRAY, MAP, STRUCT, and Composite data type
CREATE TABLE IF NOT EXISTS employee (
  name string,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

--Load data
#################################-- Note: Change the home directory path###############
!hadoop fs -put hive_employee_data/;
LOAD DATA INPATH 'hive_employee_data/employee.txt' OVERWRITE INTO TABLE employee;

--Query the whole table
SELECT * FROM employee;

--Query the ARRAY in the table
SELECT work_place FROM employee;

SELECT work_place[0] AS col_1, work_place[1] AS col_2, work_place[2] AS col_3 FROM employee;

--Query the STRUCT in the table
SELECT gender_age FROM employee;

SELECT gender_age.gender, gender_age.age FROM employee;

--Query the MAP in the table
SELECT skills_score FROM employee;

SELECT name, skills_score['DB'] AS DB,
skills_score['Perl'] AS Perl, skills_score['Python'] AS Python,
skills_score['Sales'] as Sales, skills_score['HR'] as HR FROM employee;

SELECT depart_title FROM employee;

SELECT name, depart_title['Product'] AS Product, depart_title['Test'] AS Test,
depart_title['COE'] AS COE, depart_title['Sales'] AS Sales
FROM employee;

SELECT name,
depart_title['Product'][0] AS product_col0,
depart_title['Test'][0] AS test_col0
FROM employee;

--Hive Database DDL

--Hive Table DDL

--Create internal table and load the data
CREATE TABLE IF NOT EXISTS employee_internal (
  name string,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
COMMENT 'This is an internal table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;


!hadoop fs -put hive_employee_data/;

LOAD DATA INPATH 'hive_employee_data/employee.txt' OVERWRITE INTO TABLE employee_internal;

select * from employee_internal;

!hadoop fs -put hive_employee_data/employee.txt hive_employee_data/employee.txt;

!hadoop fs -ls hive_employee_data/employee.txt;
!hadoop fs -ls hive_employee_data;

drop table IF EXISTS employee_external;

!hadoop fs -rm hive_employee_data/employee_table;

--drop table employee_external;
#################################-- -- Note: Change the directory name from to specify your home directory
--Create external table and load the data
CREATE EXTERNAL TABLE IF NOT EXISTS employee_external (
   name string,
   work_place ARRAY<string>,
   gender_age STRUCT<gender:string,age:int>,
   skills_score MAP<string,int>,
   depart_title MAP<STRING,ARRAY<STRING>>
)
COMMENT 'This is an external table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/user/u20/hive_employee_data/employee_table';

LOAD DATA INPATH 'hive_employee_data/employee.txt' OVERWRITE INTO TABLE employee_external;

select * from employee_external;


--Create Table With Data - CREATE TABLE AS SELECT (CTAS)
CREATE TABLE ctas_employee AS SELECT * FROM employee_external;

--Temporary tables
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emp1 (
name string,
work_place ARRAY<string>,
gender_age STRUCT<gender:string,age:int>,
skills_score MAP<string,int>,
depart_title MAP<STRING,ARRAY<STRING>>
);

CREATE TEMPORARY TABLE tmp_emp2 AS SELECT * FROM tmp_emp1;

CREATE TEMPORARY TABLE tmp_emp3 LIKE tmp_emp1;

--Create Table As SELECT (CTAS) with Common Table Expression (CTE)
CREATE TABLE cte_employee AS
WITH r1 AS (SELECT name FROM r2 WHERE name = 'Umesh'),
r2 AS (SELECT name FROM employee WHERE gender_age.gender= 'Male'),
r3 AS (SELECT name FROM employee WHERE gender_age.gender= 'Female')
SELECT * FROM r1 UNION ALL select * FROM r3;

SELECT * FROM cte_employee;

--Create Table Without Data - TWO ways
--With CTAS
CREATE TABLE empty_ctas_employee AS SELECT * FROM employee_internal WHERE 1=2;

--With LIKE Faster as it does not trigger Map Reduce job
CREATE TABLE empty_like_employee LIKE employee_internal;

--Check row count for both tables
SELECT COUNT(*) AS row_cnt FROM empty_ctas_employee;
SELECT COUNT(*) AS row_cnt FROM empty_like_employee;

--Show tables
SHOW TABLES;
SHOW TABLES '*emp*';

--Show columns
SHOW COLUMNS IN employee_internal;
DESC employee_internal;

--Show DDL and property
SHOW CREATE TABLE employee_internal;
SHOW TBLPROPERTIES employee_internal;

--Drop table
DROP TABLE IF EXISTS empty_ctas_employee;

DROP TABLE IF EXISTS empty_like_employee;

--Truncate table
SELECT * FROM cte_employee;

TRUNCATE TABLE cte_employee;

SELECT * FROM cte_employee;

show tables;

--Alter table statements
--Alter table name
ALTER TABLE cte_employee RENAME TO cte_employee_backup;
ALTER TABLE cte_employee_backup RENAME TO cte_employee;

--Alter table properties, such as comments
ALTER TABLE cte_employee SET TBLPROPERTIES ('comment' = 'New comments');

show tables;

--Alter columns
--Change column type - before changes
DESC employee_internal;

--Change column type
ALTER TABLE employee_internal CHANGE name employee_name string;

--Verify the changes
DESC employee_internal;

--Change column type
ALTER TABLE employee_internal CHANGE employee_name name string FIRST;

--Verify the changes
DESC employee_internal;

SHOW TABLES;

--Add/Replace Columns-before add
DESC ctas_employee;

--Add columns to the table
ALTER TABLE ctas_employee ADD COLUMNS (work string);

--Verify the added columns
DESC ctas_employee;

--Hive Partition and Buckets DDL

--Create partition table DDL
CREATE TABLE employee_partitioned
(
  name string,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
PARTITIONED BY (Year INT, Month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

--Check partition table structure
DESC employee_partitioned;

--Show partitions
SHOW PARTITIONS employee_partitioned;

--Add multiple partitions
ALTER TABLE employee_partitioned ADD
PARTITION (year=2018, month=11)
PARTITION (year=2018, month=12);

SHOW PARTITIONS employee_partitioned;

--Drop partitions
ALTER TABLE employee_partitioned DROP PARTITION (year=2018, month=11);

-- Drop all partitions in 2017;
ALTER TABLE employee_partitioned DROP IF EXISTS PARTITION (year=2017);

ALTER TABLE employee_partitioned DROP IF EXISTS PARTITION (month=9);

SHOW PARTITIONS employee_partitioned;

SHOW PARTITIONS employee_partitioned;

!hadoop fs -put hive_employee_data/employee.txt hive_employee_data/employee.txt;
!hadoop fs -ls hive_employee_data;
!hadoop fs -ls hive_employee_data/employee.txt;

--Load data to the partition
LOAD DATA INPATH 'hive_employee_data/employee.txt'
OVERWRITE INTO TABLE employee_partitioned
PARTITION (year=2018, month=12);

--Verify data loaded
SELECT name, year, month FROM employee_partitioned;

--Partition table add columns
ALTER TABLE employee_partitioned ADD COLUMNS (work string) CASCADE;

--Change data type for partition columns
ALTER TABLE employee_partitioned PARTITION COLUMN(year string);
--Verify the changes
DESC employee_partitioned;

ALTER TABLE employee_partitioned PARTITION COLUMN(year int);
DESC employee_partitioned;


ALTER TABLE employee_partitioned PARTITION (year=2018) SET FILEFORMAT ORC;

--Create a table with bucketing
--Prepare data for backet tables
CREATE TABLE employee_id
(
  name string,
  employee_id int,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

LOAD DATA INPATH 'hive_employee_data/employee_id.txt' OVERWRITE INTO TABLE employee_id;

select * from employee_id;

--Create the bucket table
CREATE TABLE employee_id_buckets
(
  name string,
  employee_id int,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
CLUSTERED BY (employee_id) INTO 2 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

set map.reduce.tasks = 2;

set hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE employee_id_buckets SELECT * FROM employee_id;

select * from employee_id_buckets;

--Hive View DDL

--Create Hive view
CREATE VIEW employee_skills
AS
SELECT name, skills_score['DB'] AS DB,
skills_score['Perl'] AS Perl, skills_score['Python'] AS Python,
skills_score['Sales'] as Sales, skills_score['HR'] as HR
FROM employee;

--Show views
SHOW VIEWS;
SHOW VIEWS 'employee_*';
DESC FORMATTED employee_skills;
SHOW CREATE TABLE employee_skills;

--Alter views properties
ALTER VIEW employee_skills SET TBLPROPERTIES ('comment' = 'This is a view');

--Redefine views
ALTER VIEW employee_skills AS SELECT * from employee ;

--Drop views
DROP VIEW employee_skills;

select * from employee_internal;

--Lateralview
SELECT name, workplace FROM employee_internal
LATERAL VIEW explode(work_place) wp as workplace;
