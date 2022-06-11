--Hive data manupulation

!hadoop fs -put hive_employee_data/employee.txt hive_employee_data/employee.txt;
!hadoop fs -ls hive_employee_data;
!hadoop fs -ls hive_employee_data/employee.txt;

--Create partition table DDL.
--Load local data to table
LOAD DATA LOCAL INPATH 'hive_employee_data/employee_hr.txt' OVERWRITE INTO TABLE employee_hr;

--Load HDFS data to table using default system path
LOAD DATA INPATH 'hive_employee_data/employee.txt'
OVERWRITE INTO TABLE employee;

--Data Exchange - INSERT
--Check the target table
SELECT name, work_place, gender_age FROM employee;

--Insert specified columns
-- Create a test table only has primary types
CREATE TABLE emp_simple(
name string,
work_place string
);

-- Specify which columns to insert
INSERT INTO TABLE emp_simple(name) 
SELECT name FROM employee WHERE name = 'Will';

--Insert values
INSERT INTO TABLE emp_simple VALUES ('Umesh', 'Toronto'),('Lucy', 'Montreal');
SELECT * FROM emp_simple;


--Export data and metadata of table
EXPORT TABLE employee TO '/tmp/output5';

!hadoop fs -ls /tmp;

--ORDER, SORT
SELECT name FROM employee ORDER BY name DESC;

--Use more than 1 reducer
SET mapred.reduce.tasks = 2;

SELECT name FROM employee SORT BY name DESC;

--Use only 1 reducer
SET mapred.reduce.tasks = 1;

SELECT name FROM employee SORT BY name DESC;

--Complex datatype function
SELECT
size(work_place) AS array_size,
size(skills_score) AS map_size,
size(depart_title) AS complex_size,
size(depart_title["Product"]) AS nest_size
FROM employee;

--Arrary functions
SELECT array_contains(work_place, 'Toronto') AS is_Toronto, sort_array(work_place) AS sorted_array FROM employee;

--virtual columns
SELECT INPUT__FILE__NAME,BLOCK__OFFSET__INSIDE__FILE AS OFFSIDE FROM employee;

--Transactions
--Below configuration parameters must be set appropriately to turn on transaction support in Hive.
SET hive.support.concurrency = true;
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.compactor.initiator.on = true;
SET hive.compactor.worker.threads = 1;

--create table support transaction
CREATE TABLE employee_trans (
emp_id int,
name string,
start_date date,
quit_date date,
quit_flag string
)
CLUSTERED BY (emp_id) INTO 2 BUCKETS STORED AS ORC
TBLPROPERTIES ('transactional'='true');

--Populate data
INSERT INTO TABLE employee_trans VALUES
(100, 'Umesh', '2017-02-01', null, 'N'),
(101, 'Will', '2017-03-01', null, 'N'),
(102, 'Amit', '2022-06-01', null, 'N'),
(104, 'Lucy', '2017-10-01', null, 'N');

--Update
UPDATE employee_trans SET quit_date = current_date, quit_flag = 'Y' WHERE emp_id = 104;
SELECT quit_date, quit_flag FROM employee_trans WHERE emp_id = 104;

--Delete
DELETE FROM employee_trans WHERE emp_id = 104;
SELECT * FROM employee_trans WHERE emp_id = 104;
SELECT * FROM employee_trans;

--Merge
--prepare another table
CREATE TABLE employee_update (
emp_id int,
name string,
start_date date,
quit_date date,
quit_flag string
);
-- Populate data
INSERT INTO TABLE employee_update VALUES
(100, 'Umesh', '2017-02-01', '2022-06-01', 'Y'), -- People quite
(102, 'Amit', '2018-01-02', null, 'N'), -- People has start_date update
(105, 'Lily', '2018-04-01', null, 'N') -- People newly started
;


-- Do a data merge from employee_update to employee_trans
MERGE INTO employee_trans as tar USING employee_update as src
ON tar.emp_id = src.emp_id
WHEN MATCHED and src.quit_flag <> 'Y' THEN UPDATE SET start_date = src.start_date
WHEN MATCHED and src.quit_flag = 'Y' THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (src.emp_id, src.name, src.start_date, src.quit_date, src.quit_flag);

--Show avaliable transactions
SHOW TRANSACTIONS;

--Show locks
SHOW LOCKS;
