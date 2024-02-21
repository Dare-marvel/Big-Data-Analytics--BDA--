## Using Inbuilt Jar File:

1. **Create a directory in HDFS:**
    ```bash
    hadoop fs -mkdir -p /user/abc/input
    ```

2. **Upload a file to HDFS:**
    ```bash
    hadoop fs -put /path/to/local/file/Kane-Williamson-All-International-Cricket-Centuries.csv /user/abc/input
    ```

3. **Run a MapReduce job using the Hadoop examples JAR:**
    ```bash
    hadoop jar /path/to/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar wordcount input/Kane-Williamson-All-International-Cricket-Centuries.csv output/pp
    ```

4. **Download the output file from HDFS to a local directory:**
    ```bash
    hdfs dfs -get /user/abc/output/pp/part-r-00000 /path/to/local/output/outt.txt
    ```

Make sure to replace `/path/to/local/file/` with the actual path to your local CSV file, and `/path/to/hadoop/` with the actual path where Hadoop is installed on your Ubuntu machine. Also, ensure that the Hadoop JAR file path is correct.

## Writing a Java Program of our own

### [Reference Video Link](https://youtu.be/6sK3LDY7Pp4?si=v8suLG_6uL6r7_Xc)

1. **Start all Hadoop daemons:**
    ```bash
    start-all.sh
    ```
    - **Description:** Initiates all Hadoop daemons, including the NameNode, DataNode, ResourceManager, and NodeManager.

2. **List Java processes:**
    ```bash
    jps
    ```
    - **Description:** Lists Java processes to check if Hadoop daemons are running.


3. **Create a directory in HDFS:**
    ```bash
    hadoop fs -mkdir /WordCountJob
    ```
    ```bash
    hadoop fs -mkdir /WordCountJob/Input
    ```
    - **Description:** Creates a directory named "WordCountJob" in the Hadoop Distributed File System (HDFS).

4. **Set HADOOP_CLASSPATH:**
    ```bash
    export HADOOP_CLASSPATH=$(hadoop classpath)
    ```
    - **Description:** Sets the `HADOOP_CLASSPATH` environment variable to include the Hadoop classpath.

5. **Print HADOOP_CLASSPATH:**
    ```bash
    echo $HADOOP_CLASSPATH
    ```
    - **Description:** Prints the value of the `HADOOP_CLASSPATH` environment variable.
      
6. **Upload a local file to HDFS:**
    ```bash
    hadoop fs -put /home/hadoop/Desktop/WordCountJob/input_data/Youtuber.txt /WordCountJob/Input
    ```
    - **Description:** Uploads the local file "Youtuber.txt" to the HDFS directory "/WordCountJob/Input."

7. **Navigate to project directory:**
    ```bash
    cd /home/hadoop/Desktop/WordCountJob/
    ```
    - **Description:** Changes the current working directory to the project directory.

8. **Compile Java code:**
    ```bash
    javac -classpath ${HADOOP_CLASSPATH} -d /home/hadoop/Desktop/WordCountJob/classes /home/hadoop/Desktop/WordCountJob/WordCount.java
    ```
    - **Description:** Compiles the Java code "WordCount.java" with the Hadoop classpath and stores the compiled classes in the "classes" directory.

9. **Create a JAR file:**
    ```bash
    jar -cvf firstProgram.jar -C classes/ .
    ```
    - **Description:** Creates a JAR file named "firstProgram.jar" containing the compiled classes from the "classes" directory.

10. **Run Hadoop MapReduce job:**
    ```bash
    hadoop jar /home/hadoop/Desktop/WordCountJob/firstProgram.jar WordCount /WordCountJob/Input /WordCountJob/Output
    ```
    - **Description:** Executes a Hadoop MapReduce job using the JAR file "firstProgram.jar" with the "WordCount" class, taking input from "/WordCountJob/Input" and producing output in "/WordCountJob/Output."

11. **Display output from HDFS:**
    ```bash
    hdfs dfs -cat /WordCountJob/Output/*
    ```
    - **Description:** Displays the content of the output files in the HDFS directory "/WordCountJob/Output."

## In the above process if you make any mistake and you want to delete the directory created you can use this command
- **Delete a directory in HDFS:**
    ```bash
    hadoop fs -rm -r /WordCountJob/Output/
    ```
    - **Description:** Deletes the directory "/WordCountJob/Output/" and its contents from HDFS if a mistake is made and you want to remove the output data.
