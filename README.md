# Kylin Spark Thriftserver Project
## Spark SQL

This module provides support for executing relational queries expressed in either SQL or the DataFrame/Dataset API.

Spark SQL is broken up into four subprojects:
 - Catalyst (sql/catalyst) - An implementation-agnostic framework for manipulating trees of relational operators and expressions.
 - Execution (sql/core) - A query planner / execution engine for translating Catalyst's logical query plans into Spark RDDs.  This component also includes a new public interface, SQLContext, that allows users to execute SQL or LINQ statements against existing RDDs and Parquet files.
 - Hive Support (sql/hive) - Includes an extension of SQLContext called HiveContext that allows users to write queries using a subset of HiveQL and access data from a Hive Metastore using Hive SerDes.  There are also wrappers that allows users to run queries that include Hive UDFs, UDAFs, and UDTFs.
 - HiveServer and CLI support (sql/hive-thriftserver) - Includes support for the SQL CLI (bin/spark-sql) and a HiveServer2 (for JDBC/ODBC) compatible server.
 
## Kylin Spark Thriftserver Project
Kylin Spark Thriftserver Project, implement Thriftserver HA with zookeeper

**该项目改进：**  
1、ThriftServer HA 
2、client JDBC使用zookeeper连接串
3、不同用户提交的sql跑在自己的队列  

## Requirements
目前已经在以下版本中测试发布：
> Java version = 1.8.0_60  
> Scala version = 2.11.8  
> Spark version = 2.1.1  
> Hive version =  1.2.1  
> Hadoop version = 2.7.3  



