hdfs dfs -rm -r output
yarn jar lab1/target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output
hdfs dfs -copyToLocal output
hdfs dfs -text output/part-r-00000 > result.txt
