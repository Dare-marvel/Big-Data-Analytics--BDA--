# Performing CRUD operations on Apache HBase

## [Click here to view the tutorial of installation of Hive on Ubuntu ](https://www.youtube.com/watch?v=wPIawRML168)

## Starting Hadoop Daemons
```shell
start-all.sh
```

## Verify Running Processes with jps command
```shell
jps
```

## Start Hive Shell:
```shell
hive
```

## Performing Queries

### 1. Create Table:
* **Command:** 
```sql
CREATE TABLE table_name (
    column_name1 data_type1,
    column_name2 data_type2,
    ...
);
```
* **Example:**
```sql
CREATE TABLE cricket_matches (
    match_id INT,
    team1 STRING,
    team2 STRING,
    match_date DATE,
    venue STRING,
    result STRING
);
```
* **Explanation:** 
This blueprint sketches out the foundation for any data drama, with `table_name` ready to be customized to your narrative needs.

### 2. Insert Data :
* **Command:** 
```sql
INSERT INTO table_name VALUES (value1, value2, ...);
```
* **Example:**
```sql
INSERT INTO cricket_matches VALUES
(1, 'India', 'Australia', '2024-03-25', 'Melbourne Cricket Ground', 'India won by 50 runs');
```
* **Explanation:** 
Breathe life into your table with stories and characters, each value taking its place in the ongoing saga.

### 3. Select Data:
* **Command:** 
```sql
SELECT * FROM table_name;
```
* **Example:**
```sql
SELECT * FROM cricket_matches;
```
* **Explanation:** 
A grand reveal, pulling back the curtain to unveil all that lies within `table_name`.

### 4. Alter Table (Add Column):
* **Command:** 
```sql
ALTER TABLE table_name ADD COLUMNS (new_column_name column_type);
```
* **Example:**
```sql
ALTER TABLE cricket_matches ADD COLUMNS (winning_team STRING);
```
* **Explanation:** 
Adapting the stage for new narratives, introducing fresh elements to enrich the tale.

### 5.  Insert Overwrite:
* **Command:** 
```sql
INSERT OVERWRITE TABLE table_name SELECT ... ;
```
* **Example:**
```sql
INSERT OVERWRITE TABLE cricket_matches
SELECT match_id, team1, team2, match_date, venue, result,
       CASE 
           WHEN result LIKE '%India%' THEN 'India'
           ELSE 'Unknown'
       END AS winning_team
FROM cricket_matches;
```
* **Explanation:** 
A narrative pivot, redefining outcomes with newfound insights, reshaping the story within `table_name`.

### 6. Create Table As Select:
* **Command:** 
```sql
CREATE TABLE new_table AS SELECT * FROM table_name WHERE condition;
```
* **Example:**
```sql
CREATE TABLE cricket_matches_new AS
SELECT *
FROM cricket_matches
WHERE match_id <> 1;
```
* **Explanation:** 
From the ashes of the old, a new narrative arises, selectively inheriting the legacy of `table_name`.

### 7. Drop Table:
* **Command:** 
```sql
DROP TABLE table_name;
```
* **Example:**
```sql
DROP TABLE cricket_matches;
```
* **Explanation:** 
An end to one story, making room for new tales to unfold, erasing `table_name` from our saga.

### 8. Rename Table (Generic):
* **Command:** 
```sql
ALTER TABLE old_name RENAME TO new_name;
```
* **Example:**
```sql
ALTER TABLE cricket_matches_new RENAME TO cricket_matches;
```
* **Explanation:** 
A tale of transformation, as `old_name` sheds its skin to emerge anew as `new_name`.

### 9. Show Tables:
* **Command:** 
```sql
SHOW TABLES;
```
* **Example:**
```sql
SHOW TABLES;
```
* **Explanation:** 
A glimpse behind the scenes, revealing all the stages set for our stories to play out.

### 10. Load CSV Data:
* **Initial Step:** Move your CSV to HDFS
```shell
hadoop fs -put /local/path/to/yourfile.csv /user/hive/warehouse/table_name/
```
* **Load Command:** 
```sql
LOAD DATA INPATH '/user/hive/warehouse/table_name/yourfile.csv' INTO TABLE table_name;
```
* **Explanation:** 
Transports your cast of characters from the mundane realm of CSV into the spotlight of `table_name`, ready to unfold their tales.

