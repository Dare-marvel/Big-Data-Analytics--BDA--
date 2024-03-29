# Performing CRUD operations on Apache HBase

## Starting Hadoop Daemons
```shell
start-all.sh
```

## Starting HBase
```shell
start-hbase.sh
```

## Verify Running Processes with jps command
```shell
jps
```

## Start HBase Shell
```shell
hbase shell
```

## Performing Queries

## 1. Create Table:
* **Command:** 
```shell
create 'tableName', 'columnFamily'
```
* **Example:**
```shell
create 'students', 'info'
```
* **Explanation:** This command creates a new table in HBase with the specified table name and column family. Column family is a way to group related columns together for efficient storage and retrieval.

## 2. Put Data:
* **Command:** 
```shell
put 'tableName', 'rowKey', 'columnFamily:columnQualifier', 'value'
```
* **Example:** 
```shell
put 'students', '1001', 'info:name', 'John Doe'
```
* **Explanation:** 
This command inserts or updates a value for a specific column in a row. You specify the table name, row key, column family, column qualifier, and the value you want to insert/update.

## 3. Get Data:
* **Command:** 
```shell
get 'tableName', 'rowKey'
```
* **Example:** 
```shell
get 'students', '1001'
```
* **Explanation:** 
This command retrieves data for a specific row from the table. You specify the table name and the row key, and it returns all columns and their values associated with that row.

## 4. Delete Data:
* **Command:** 
```shell
delete 'tableName', 'rowKey', 'columnFamily:columnQualifier'
```
* **Example:** 
```shell
delete 'students', '1001', 'info:name'
```
* **Explanation:** 
This command deletes a specific column from a row. You specify the table name, row key, and the column family along with column qualifier that you want to delete.

## 5. Delete All Data from a Row:
* **Command:** 
```shell
deleteall 'tableName', 'rowKey'
```
* **Example:** 
```shell
deleteall 'students', '1001'
```
* **Explanation:** 
This command deletes all columns and their values associated with a specific row. It effectively removes all data for that row.

**Note:** Replace 'tableName', 'rowKey', 'columnFamily', 'columnQualifier', and 'value' with actual names and values as per your data structure and requirements.

