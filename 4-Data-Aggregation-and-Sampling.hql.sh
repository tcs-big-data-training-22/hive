--Hive Data Aggregation and Sampling

--Aggregation without GROUP BY columns
SELECT count(*) as rowcnt1, count(1) AS rowcnt2 FROM employee;

--Aggregation with GROUP BY columns
SELECT gender_age.gender, count(*) AS row_cnt FROM employee
GROUP BY gender_age.gender;

--Aggregate functions can be also used with DISTINCT keyword to do aggregation on unique values.
SELECT count(distinct gender_age.gender) AS gender_uni_cnt, count(distinct name) AS name_uni_cnt FROM employee;

--Use max/min struct
SELECT gender_age.gender,
max(struct(gender_age.age, name)).col1 as age,
max(struct(gender_age.age, name)).col2 as name
FROM employee
GROUP BY gender_age.gender;


--Aggregation condition – HAVING
SELECT gender_age.age, count(*) as cnt FROM employee GROUP BY gender_age.age HAVING cnt=1;

--Prepare table and data
CREATE TABLE IF NOT EXISTS employee_contract
(
name string,
dept_num int,
employee_id int,
salary int,
type string,
start_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA INPATH
'hive_employee_data/employee_contract.txt'
OVERWRITE INTO TABLE employee_contract;
