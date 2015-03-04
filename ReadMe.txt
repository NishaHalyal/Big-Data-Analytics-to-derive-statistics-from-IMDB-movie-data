Steps for Exection:

1) All jar files have same name as class name
2) Need to include extra jars: 1> hadoop-common-2.6.0 
							   2> hadoop-mapreduce-client-core-2.6.0
3) Transfer all required files from local system to VM
4) Run the following commands:

For Question 1:
hdfs dfs -mkdir input1
hdfs dfs -put users.dat input1
hadoop jar Question1.jar Question1 input1 output1

For Question2:
hdfs dfs -mkdir input1
hdfs dfs -put users.dat input1
hadoop jar Question2.jar Question1 input1 output2

For Question3:
hdfs dfs -mkdir input3
hdfs dfs -put movies.dat input3
hadoop jar Question3.jar Question1 input3 output3 Action