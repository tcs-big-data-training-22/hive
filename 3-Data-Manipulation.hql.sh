--Hive data manupulation

exit;
hadoop fs -put hive_employee_data/
hadoop fs -ls /user/$USER/hive_employee_data
hadoop fs -ls /user/$USER/hive_employee_data/employee.txt

hive

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
CREATE TABLE emp_simple( -- Create a test table only has primary types
name string,
work_place string
);

INSERT INTO TABLE emp_simple(name) -- Specify which columns to insert
SELECT name FROM employee WHERE name = 'Will';

--Insert values
INSERT INTO TABLE emp_simple VALUES ('Umesh', 'Toronto'),('Lucy', 'Montreal');
SELECT * FROM emp_simple;


--Export data and metadata of table
EXPORT TABLE employee TO '/tmp/output5';

--dfs -ls -R /tmp/output5/

--Import table with the same name
IMPORT FROM '/tmp/output5';

--ORDER, SORT
SELECT name FROM employee ORDER BY name DESC;

--Use more than 1 reducer
SET mapred.reduce.tasks = 2;

SELECT name FROM employee SORT BY name DESC;

--Use only 1 reducer
SET mapred.reduce.tasks = 1;

SELECT name FROM employee SORT BY name DESC;

--Distribute by
SELECT name, employee_id
FROM employee_hr DISTRIBUTE BY employee_id ;

--Used with SORT BY
SELECT name, start_date FROM employee_hr DISTRIBUTE BY start_date SORT BY name;

--Cluster by
SELECT name, employee_id FROM employee_hr CLUSTER BY name ;

--Complex datatype function
SELECT
size(work_place) AS array_size,
size(skills_score) AS map_size,
size(depart_title) AS complex_size,
size(depart_title["Product"]) AS nest_size
FROM employee;

SELECT size(null), size(array(null)), size(array());

--Arrary functions
SELECT array_contains(work_place, 'Toronto') AS is_Toronto, sort_array(work_place) AS sorted_array FROM employee;

--Date and time functions
SELECT to_date(from_unixtime(unix_timestamp())) AS currentdate;

--To compare the difference of two date.
SELECT (unix_timestamp('2018-01-21 18:00:00') - unix_timestamp('2018-01-10 11:00:00'))/60/60/24 AS daydiff;

--Get the file name form a Linux path
SELECT reverse(split(reverse('/home/user/employee.txt'),'/')[0]) AS linux_file_name;

--collect set or list
SELECT
collect_set(gender_age.gender) AS gender_set,
collect_list(gender_age.gender) AS gender_list
FROM employee;

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
