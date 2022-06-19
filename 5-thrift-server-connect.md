# Thrift Server connect

## Reference:
- https://docs.microsoft.com/en-us/azure/hdinsight/hadoop/apache-hadoop-connect-hive-jdbc-driver

## JDBC connection string
```
jdbc:hive2://CLUSTERNAME.azurehdinsight.net:443/default;transportMode=http;ssl=true;httpPath=/hive2
```

## Host name in connection string
- Host name 'CLUSTERNAME.azurehdinsight.net' in the connection string is the same as your cluster URL
- You can get it through Azure portal.


## Port in connection string
- You can only use port 443 to connect to the cluster from some places outside of the Azure virtual network
- HDInsight is a managed service, which means all connections to the cluster are managed via a secure Gateway. You can't connect to HiveServer 2 directly on ports 10001 or 10000
- These ports aren't exposed to the outside.

## Authentication
- When establishing the connection, use the HDInsight cluster admin name and password to authenticate
- From JDBC clients such as SQuirreL SQL, enter admin name and password in client settings.


