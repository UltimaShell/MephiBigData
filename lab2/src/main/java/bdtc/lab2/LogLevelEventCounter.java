package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import static org.apache.spark.sql.functions.sum;

@AllArgsConstructor
@Slf4j
public class LogLevelEventCounter {

    // Формат времени логов - н-р, '1973-11-30'
    private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .toFormatter(Locale.ENGLISH);

    private static final DateTimeFormatter formatter2 = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss.S")
            .toFormatter(Locale.ENGLISH);
    /**
     * Функция подсчета количества публикаций автора в год по вузам
     * @param inputDataset - входной Dataset публикаций для анализа
     * @return результат подсчета в формате Dataset
     */

    public static Dataset<Row> countLogLevelPerHour(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());
        Dataset<Publications> logLevelHourDataset = words.map(s -> {
            String Field=s.substring(1, s.length() - 1);
            String[] logFields = Field.split(",");
            LocalDate date = LocalDate.parse(logFields[3], formatter);
            int university=Integer.parseInt(logFields[0]);
            int autor=Integer.parseInt(logFields[1]);
            return new Publications(date.getYear(),autor,university);
            }, Encoders.bean(Publications.class))
                .coalesce(1);

        Dataset<Row> t1 = logLevelHourDataset.groupBy("year", "userId", "univId")
                .count()
                .toDF("year", "univId", "userId", "count")
                .sort(functions.asc("year"));
        return t1;
    }

    /**
     * Функция подсчета количества часов проведенных сотрудником/студентом в год по вузам
     * @param inputDataset - входной Dataset проходов в вуз/из вуза для анализа
     * @return результат подсчета в формате Dataset
     */
    public static Dataset<Row> countTimeInUniver(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());
        Dataset<Journal> logLevelHourDataset = words.map(s -> {
            String Field=s.substring(1, s.length() - 1);
            String[] logFields = Field.split(",");
            int university=Integer.parseInt(logFields[0]);
            int autor=Integer.parseInt(logFields[1]);
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
            Date d = format.parse(logFields[2]);

            LocalDate dat = LocalDate.parse(logFields[2], formatter2);

            long ddd = d.getTime();
            int year = dat.getYear();
            boolean tipo= Boolean.parseBoolean(logFields[3]);
            if (tipo){
                ddd= -ddd;
            }
            return new Journal(year,autor,university, ddd);
        }, Encoders.bean(Journal.class))
                .coalesce(1);

        Dataset<Row> t2 = logLevelHourDataset.groupBy("year", "univId", "userId")
                .agg(sum("checktime").divide(3600000))//milliseconds
                .drop("checktime")
                .toDF("year", "univId", "userId", "sum")
                .sort(functions.asc("year"));

        return t2;
    }

    /**
     * Функция объединения результатов двух предидущих
     * @param inputPub - входной Dataset публикаций для анализа
     * @param inputJournal - входной Dataset посещений для анализа
     * @return результат подсчета в формате JavaRDD
     */

    public static JavaRDD<Row> allCompute(Dataset<Row> inputPub, Dataset<Row> inputJournal) {
        Dataset<Row> zx = inputJournal
                .union(inputPub)
                .drop("sum")
                .distinct()
                .withColumnRenamed("year", "god")
                .withColumnRenamed("univId", "univer")
                .withColumnRenamed("userId", "user");

        Dataset<Row> zc = zx.join(inputJournal, zx.col("god").equalTo(inputJournal.col("year"))
                .and(zx.col("univer").equalTo(inputJournal.col("univId")))
                .and(zx.col("user").equalTo(inputJournal.col("userId"))), "full")
                .join(inputPub, zx.col("god").equalTo(inputPub.col("year"))
                .and(zx.col("univer").equalTo(inputPub.col("univId")))
                .and(zx.col("user").equalTo(inputPub.col("userId"))), "full")
                .drop("year")
                .drop("univId")
                .drop("userId");

        return zc.toJavaRDD();
    }
}
