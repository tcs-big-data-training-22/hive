--Extensibility

--UDF deployment
CREATE TEMPORARY FUNCTION tmptoUpper
as 'com.packtpub.hive.essentials.hiveudf.toupper';
USING JAR 'hdfs:///app/hive/function/hiveudf-1.0.jar';

CREATE FUNCTION toUpper
as 'hive.essentials.hiveudf.ToUpper'
USING JAR 'hdfs:///app/hive/function/hiveudf-1.0.jar';

SHOW FUNCTIONS ToUpper;
DESCRIBE FUNCTION ToUpper;
DESCRIBE FUNCTION EXTENDED ToUpper;

RELOAD FUNCTION;

SELECT name, toUpper(name) as cap_name, tmptoUpper(name) as cname FROM employee;

DROP TEMPORARY FUNCTION IF EXISTS tmptoUpper;
DROP FUNCTION IF EXISTS toUpper;

--Streaming, call the script in Hive CLI from HQL.
ADD FILE /tmp/upper.py;
SELECT TRANSFORM (name,work_place[0])
USING 'python upper.py' AS (CAP_NAME,CAP_PLACE)
FROM employee;
