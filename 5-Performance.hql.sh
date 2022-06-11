--Performance Considerations
--Query explain
EXPLAIN SELECT gender_age.gender, count(*) FROM employee_partitioned WHERE year=2018 GROUP BY gender_age.gender LIMIT 2;

--ANALYZE statement
ANALYZE TABLE employee COMPUTE STATISTICS;

--Check the statistics
DESCRIBE EXTENDED employee;

DESCRIBE FORMATTED employee;

SET hive.stats.autogather=ture;
