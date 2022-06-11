--Performance Considerations
--Query explain
EXPLAIN SELECT gender_age.gender, count(*) FROM employee_partitioned WHERE year=2018 GROUP BY gender_age.gender LIMIT 2;

--ANALYZE statement
ANALYZE TABLE employee COMPUTE STATISTICS;

ANALYZE TABLE employee COMPUTE STATISTICS NOSCAN;

ANALYZE TABLE employee_id COMPUTE STATISTICS FOR COLUMNS employee_id;

--Check the statistics
DESCRIBE EXTENDED employee;

DESCRIBE FORMATTED employee;

SET hive.stats.autogather=ture;

--Use screw tables
CREATE TABLE sample_skewed_table (
dept_no int,
dept_name string
)
SKEWED BY (dept_no) ON (1000, 2000);

DESC FORMATTED sample_skewed_table;

--Data file optimization
--File format
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;

--Compression
SET hive.exec.compress.intermediate=true;
SET hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec

--Storage optimization
SET hive.exec.mode.local.auto=true;
SET hive.exec.mode.local.auto.inputbytes.max=50000000;
SET hive.exec.mode.local.auto.input.files.max=5;

--JVM reuse
SET mapreduce.job.jvm.numtasks=5;

--Parallel running job
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;

--Map Join
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=600000000;
SET hive.auto.convert.join.noconditionaltask = true;
SET hive.auto.convert.join.noconditionaltask.size = 10000000;

--Bucket Map Join
SET hive.auto.convert.join=true;
SET hive.optimize.bucketmapjoin=true;

--Sort Merge Bucket (SMB) Join
SET hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
SET hive.auto.convert.sortmerge.join.noconditionaltask=true;

--Sort Merge Bucket Map Join
SET hive.auto.convert.join=true;
SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
SET hive.auto.convert.sortmerge.join.noconditionaltask=true;
SET hive.auto.convert.sortmerge.join.bigtable.selection.policy=org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

--Skew Join
SET hive.optimize.skewjoin=true;
SET hive.skewjoin.key=100000;

--Skew data in GROUP BY
SET hive.groupby.skewindata=true;