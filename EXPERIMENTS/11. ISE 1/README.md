## [Dataset Link](https://www.kaggle.com/datasets/shiivvvaam/top-youtuber-worldwide)

### Using Hadoop Examples JAR:

```bash
# Set your local CSV file path
LOCAL_CSV_FILE="/path/to/local/file/Kane-Williamson-All-International-Cricket-Centuries.csv"

# Set your HDFS directory path
HDFS_DIRECTORY="/user/abc"

# Set your Hadoop JAR file path
HADOOP_JAR_PATH="/path/to/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar"

# 1. Create a directory in HDFS
hadoop fs -mkdir -p $HDFS_DIRECTORY/input

# 2. Upload a file to HDFS
hadoop fs -put $LOCAL_CSV_FILE $HDFS_DIRECTORY/input

# 3. Run a MapReduce job using the Hadoop examples JAR
hadoop jar $HADOOP_JAR_PATH wordcount $HDFS_DIRECTORY/input/Kane-Williamson-All-International-Cricket-Centuries.csv $HDFS_DIRECTORY/output/pp

# 4. Download the output file from HDFS to a local directory
hdfs dfs -get $HDFS_DIRECTORY/output/pp/part-r-00000 /path/to/local/output/outt.txt
```

### Writing a Custom Java Program:

```bash
# Set your local input file path
LOCAL_INPUT_FILE="/path/to/local/file/Youtuber.txt"

# Set your HDFS directory path
HDFS_DIRECTORY="/yourfilenameJob"

# Set your project directory
PROJECT_DIRECTORY="/home/hadoop/Desktop/yourfilenameJob"

# 1. Start all Hadoop daemons
start-all.sh

# 2. List Java processes
jps

# 3. Create directories in HDFS
hadoop fs -mkdir $HDFS_DIRECTORY
hadoop fs -mkdir $HDFS_DIRECTORY/Input

# 4. Set HADOOP_CLASSPATH
export HADOOP_CLASSPATH=$(hadoop classpath)

# 5. Print HADOOP_CLASSPATH
echo $HADOOP_CLASSPATH

# 6. Upload a local file to HDFS
hadoop fs -put $LOCAL_INPUT_FILE $HDFS_DIRECTORY/Input

# 7. Navigate to project directory
cd $PROJECT_DIRECTORY

# 8. Compile Java code
javac -classpath ${HADOOP_CLASSPATH} -d $PROJECT_DIRECTORY/classes $PROJECT_DIRECTORY/yourfilename.java

# 9. Create a JAR file
jar -cvf yourfilenameJob.jar -C $PROJECT_DIRECTORY/classes/ .

# 10. Run Hadoop MapReduce job
hadoop jar $PROJECT_DIRECTORY/yourfilenameJob.jar yourfilename $HDFS_DIRECTORY/Input $HDFS_DIRECTORY/Output

# 11. Display output from HDFS
hdfs dfs -cat $HDFS_DIRECTORY/Output/*

# 12. Copy the entire output directory from HDFS to the local file system
hadoop fs -get $HDFS_DIRECTORY/Output $PROJECT_DIRECTORY
```

### Deleting HDFS Output Directory:

```bash
# Delete a directory in HDFS
hadoop fs -rm -r $HDFS_DIRECTORY/Output/
```

Remember to replace "yourfilename" with your actual filename in these scripts.
Make sure to replace the placeholder paths with your actual file and directory paths.
