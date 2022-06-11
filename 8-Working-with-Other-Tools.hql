--Work with Other Tools

--Hive HBase integration
CREATE EXTERNAL TABLE hbase_table_sample(
id int,
value1 string,
value2 string,
map_value map<string, string>
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val,cf2:val,cf3:")
TBLPROPERTIES ("hbase.table.name" = "table_name_in_hbase");
