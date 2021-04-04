rm -rf output
hdfs dfs -rm -r input
hdfs dfs -rm -r output
hdfs dfs -put input input
yarn jar lab1/target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output
hdfs dfs -copyToLocal output
hdfs dfs -text output/part-r-00000 > result.txt

