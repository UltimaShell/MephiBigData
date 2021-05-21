package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Подсчитывает данные публикационной активности сотрудников и студентов университетов и статистику посещаемости организации за год
 */
@Slf4j
public class SparkSQLApplication {
    /**
     * @param args - args[0]: входной файл публикация, args[1]: входной файл посещений, args[2] - выходная папка
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar pub.file journal.file outputDirectory");
        }

        log.info("Appliction started!");
        log.debug("Application started");
        SparkSession sc = SparkSession
                .builder()
                .master("local")
                .appName("SparkSQLApplication")
                .getOrCreate();

        Dataset<String> pub = sc.read().text(args[0]).as(Encoders.STRING());
        Dataset<String> jou = sc.read().text(args[1]).as(Encoders.STRING());
        log.info("===============COUNTING...================");
        Dataset<Row> result = LogLevelEventCounter.countLogLevelPerHour(pub);
        Dataset<Row> result2 = LogLevelEventCounter.countTimeInUniver(jou);

        JavaRDD<Row> result_fin = LogLevelEventCounter.allCompute(result, result2);
        log.info("============SAVING FILE TO " + args[2] + " directory============");
        result_fin.saveAsTextFile(args[2]);
    }
}
