spark-submit --class bdtc.lab2.SparkSQLApplication --master local --deploy-mode client --executor-memory 4g --name laba2 --conf "spark.app.id=SparkSQLApplication" lab2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://127.0.0.1:9000/user/root/publication/ hdfs://127.0.0.1:9000/user/root/journal/ out
hdfs dfs -text out/part-* > result.txt
