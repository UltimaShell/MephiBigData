package bdtc.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static bdtc.lab2.LogLevelEventCounter.countLogLevelPerHour;
import static bdtc.lab2.LogLevelEventCounter.countTimeInUniver;
import static bdtc.lab2.LogLevelEventCounter.allCompute;

public class SparkTest {
    final String testString1 = "49,80,Сопровождаться художественный точно.,2006-10-20\n";;
    final String testString2 = "84,1,Сопровождаться художественный точно.,1973-11-30\n";//uid+pid+pub+date

    final String tsSt1="49,80,2006-10-20 22:06:35.0,true";//uid+pid+time+type
    final String tsSt5="49,80,2006-10-21 00:06:35.0,false";
    final String tsSt6="49,81,2006-10-20 22:06:35.0,true";//uid+pid+time+type
    final String tsSt4="49,81,2006-10-21 01:06:35.0,false";
    final String tsSt2="49,80,2006-10-20 22:06:35.0,true";//uid+pid+time+type
    final String tsSt3="49,80,2006-10-21 02:06:35.0,false";
    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    @Test
    public void testOnePub() {
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Collections.singletonList(testString2));
        Dataset<Row> result = countLogLevelPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        JavaRDD<Row> result2 = result.toJavaRDD();
        List<Row> rowList = result2.collect();

        assert rowList.iterator().next().getInt(0) == 1973;//year
        assert rowList.iterator().next().getInt(1) == 1;//univer
        assert rowList.iterator().next().getInt(2) == 84;//user
        assert rowList.iterator().next().getLong(3) == 1;//count
    }

    @Test
    public void testOneUniver() {
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(tsSt1, tsSt2, tsSt3, tsSt4, tsSt5, tsSt6));
        Dataset<Row> result = countTimeInUniver(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        JavaRDD<Row> result2 = result.toJavaRDD();
        List<Row> rowList = result2.collect();

        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);

        assert firstRow.getInt(0) == 2006;
        assert firstRow.getInt(1) == 80;
        assert firstRow.getInt(2) == 49;
        assert firstRow.getDouble(3) == 6;

        assert secondRow.getInt(0) == 2006;
        assert secondRow.getInt(1) == 81;
        assert secondRow.getInt(2) == 49;
        assert secondRow.getDouble(3) == 3;
    }

    @Test
    public void ComplexTest(){
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString2));
        Dataset<Row> result = countLogLevelPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));

        JavaRDD<String> dudu2 = sc.parallelize(Arrays.asList(tsSt1, tsSt2, tsSt3, tsSt4, tsSt5, tsSt6));
        Dataset<Row> result2 = countTimeInUniver(ss.createDataset(dudu2.rdd(), Encoders.STRING()));
        JavaRDD<Row> finalres = allCompute(result, result2);
        List<Row> rowList = finalres.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);
        Row thirdRow = rowList.get(2);

        assert firstRow.getInt(0) == 2006;
        assert firstRow.getInt(1) == 81;
        assert firstRow.getInt(2) == 49;
        assert firstRow.getDouble(3) == 3;

        assert secondRow.getInt(0) == 1973;
        assert secondRow.getInt(1) == 1;
        assert secondRow.getInt(2) == 84;
        assert secondRow.getLong(4) == 1;

        assert thirdRow.getInt(0) == 2006;
        assert thirdRow.getInt(1) == 80;
        assert thirdRow.getInt(2) == 49;
        assert thirdRow.getDouble(3) == 6;
        assert thirdRow.getLong(4) == 1;
    }
}
