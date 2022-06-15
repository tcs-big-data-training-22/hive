Partitioning
-------------
static ::
FROM page_view_stg pvs
INSERT OVERWRITE TABLE page_view PARTITION(dt='2008-06-08', country='US')
SELECT pvs.viewTime, pvs.userid, pvs.page_url, pvs.referrer_url, null, null, pvs.ip WHERE pvs.country = 'US';

Dynamic :: strict
FROM page_view_stg pvs
INSERT OVERWRITE TABLE page_view PARTITION(dt='2008-06-08', country)
SELECT pvs.viewTime, pvs.userid, pvs.page_url, pvs.referrer_url, null, null, pvs.ip, pvs.country

set hive.exec.dynamic.partition=true;    
set hive.exec.dynamic.partition.mode=nonstrict;  

FROM page_view_stg pvs
INSERT OVERWRITE TABLE page_view PARTITION(dt, country)
SELECT pvs.viewTime, pvs.userid, pvs.page_url, pvs.referrer_url, null, null, pvs.ip, from_unixtimestamp(pvs.viewTime, 'yyyy-MM-dd') ds, pvs.country;
                        
CREATE TABLE page_view(viewTime INT, userid BIGINT,
                page_url STRING, referrer_url STRING,
                ip STRING COMMENT 'IP Address of the User')
COMMENT 'This is the page view table'
PARTITIONED BY(dt STRING, country STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE page_view_stg(viewTime INT, userid BIGINT,
                page_url STRING, referrer_url STRING,
                ip STRING COMMENT 'IP Address of the User',
                country STRING COMMENT 'country of origination')
COMMENT 'This is the staging page view table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '44' LINES TERMINATED BY '12'
STORED AS TEXTFILE
LOCATION '/user/data/staging/page_view';

hadoop dfs -put /tmp/pv_2008-06-08.txt /user/data/staging/page_view


Buckting
-----------
Bucketing can also be done even without partitioning on Hive tables.
Bucketing => based on Hash function on bucketing column

set hive.enforce.bucketing = true
property sets the number of reduce tasks == number of buckets mentioned in the table definition

create table buck_emp(
    id int,
    name string,
    salary int)
CLUSTERED BY (id)
SORTED BY (id)
INTO 4 BUCKETS;

We need to use regular INSERT statement to insert into bucketed table.

INSERT OVERWRITE TABLE buck_emp
SELECT * FROM emp;



File Format:

TEXTFILE ::
supports - CSV (Comma Separated Values), delimited by Tabs, Spaces, and JSON data. 
By default, if we use TEXTFILE format then each line is considered as a record.

create table olympic(athelete STRING,age INT,country STRING,year STRING,closing STRING,sport STRING,gold INT,silver INT,bronze INT,total INT) row format delimited fields terminated by '\t' stored as textfile;

ORCFILE:
ORC stands for Optimized Row Columnar which means it can store data in an optimized way than the other file formats. ORC reduces the size of the original data up to 75%(eg: 100GB file will become 25GB).

create table olympic_orcfile(athelete STRING,age INT,country STRING,year STRING,closing STRING,sport STRING,gold INT,silver INT,bronze INT,total INT) row format delimited fields terminated by '\t' stored as orcfile;


AVRO vs PARQUET vs ORC

Avro is a row-based storage/serialization binary format
Avro supports schema evolution. Avro handles schema changes like missing fields, added fields and changed fields.


ORC - Optimized Row Columnar file
- as it stores data in columner format, which leads to effective compression 

AVRO vs PARQUET
AVRO is a row-based storage format whereas PARQUET is a columnar based storage format.
Write operations in AVRO are better than in PARQUET
PARQUET is much better for analytical querying i.e. reads and querying are much more efficient than writing.
PARQUET is ideal for querying a subset of columns in a multi-column table. AVRO is ideal in case of ETL operations where we need to query all the columns.

ORC vs PARQUET
Both are columner format
But, 
Parquet might be better if you have highly nested data, because it stores its elements as a tree-like structure.
Apache ORC might be better if your file-structure is flattened.

Spark performs best with parquet, hive performs best with ORC

Indexing:

Indexing has been removed in version 3.0

Materialized views with automatic rewriting can result in very similar results.

Hive 2.3.0 adds support for materialzed views.

- The goal of Hive indexing is to improve the speed of query lookup on certain columns of a table. Without an index, queries with predicates like 'WHERE tab1.col1 = 10' load the entire table or partition and process all the rows. 

- But if an index exists for col1, then only a portion of the file needs to be loaded and processed.

- The improvement in query speed that an index can provide comes at the cost of additional processing to create the index and disk space to store the index.

CREATE INDEX table01_index ON TABLE table01 (column2) AS 'COMPACT';

SHOW INDEX ON table01;

DROP INDEX table01_index ON table01;


View:: View is just a named query. It doesn't store anything. When there is a query on view, it runs the query of the view definition. Actual data comes from table.

Materialised Views:: Stores data physically and get updated periodically. While querying Materialised Views, it gives data from Materialised Views.

CREATE TABLE emps (
  empid INT,
  deptno INT,
  name VARCHAR(256),
  salary FLOAT,
  hire_date TIMESTAMP)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
 
CREATE TABLE depts (
  deptno INT,
  deptname VARCHAR(256),
  locationid INT)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

CREATE MATERIALIZED VIEW mv1
AS
SELECT empid, deptname, hire_date
FROM emps JOIN depts
  ON (emps.deptno = depts.deptno)
WHERE hire_date >= '2016-01-01';


CBO::
Hive Cost-Based Optimizer (CBO) is a core component in Hive query processing engine. 
CBO optimizes and calculates the cost of various plans for a query.
CBO is to generate efficient execution plans by examining the tables and conditions specified in the query.

set hive.cbo.enable=true (Enables cost-based query optimization.)
set hive.stats.autogather=true
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;

EXPLAIN :: - spark SQL or hive 
hive> explain select * from people;
spark.sql("select * from people").explain()



---------------------------------------------------------------------------------------------------------------------------------------------

HIVE DML::
-------------
Dynamic Partition Inserts:

hive.exec.dynamic.partition=true

Needs to be set to true to enable dynamic partition inserts

hive.exec.dynamic.partition.mode=strict

In strict mode, the user must specify at least one static partition, in nonstrict mode all partitions are allowed to be dynamic

FROM page_view_stg pvs
INSERT OVERWRITE TABLE page_view PARTITION(dt='2008-06-08', country)
SELECT pvs.viewTime, pvs.userid, pvs.page_url, pvs.referrer_url, null, null, pvs.ip, pvs.cnt

Here the country partition will be dynamically created by the last column from the SELECT clause (i.e. pvs.cnt).

All Hive keywords are case-insensitive, including the names of Hive operators and functions.

SHOW FUNCTIONS;

hive> DESCRIBE FUNCTION year;
OK
year(param) - Returns the year component of the date/timestamp/interval
Time taken: 0.029 seconds, Fetched: 1 row(s)

hive> DESCRIBE FUNCTION EXTENDED year;
OK
year(param) - Returns the year component of the date/timestamp/interval
param can be one of:
1. A string in the format of 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'.
2. A date value
3. A timestamp value
4. A year-month interval valueExample:
   > SELECT year('2009-07-30') FROM src LIMIT 1;
  2009
Function class:org.apache.hadoop.hive.ql.udf.UDFYear
Function type:BUILTIN
Time taken: 0.032 seconds, Fetched: 10 row(s)
hive> DESCRIBE FUNCTION EXTENDED version;
OK
version() - Returns the Hive build version string - includes base version and revision.
Function class:org.apache.hadoop.hive.ql.udf.UDFVersion
Function type:BUILTIN
Time taken: 0.026 seconds, Fetched: 3 row(s)

hive> select year('2020-12-12');
OK
2020
hive> select month('2020-12-12');
OK
12
hive> select day('2020-12-12');
OK
12
hive> select extract(month from "2016-10-20");
10
hive> select current_date;
OK
2020-12-16
hive> select current_timestamp;
OK
2020-12-16 15:35:46.17
hive> select last_day(current_date);
OK
2020-12-31
hive> select next_day('2015-01-14', 'TU');
OK
2015-01-20
hive> select explode(array('A','B','C'));
OK
A
B
C
hive> select explode(map('A',10,'B',20,'C',30)) as (key,value);
OK
A	10
B	20
C	30
