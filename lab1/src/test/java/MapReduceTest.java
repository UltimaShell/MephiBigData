import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private final String testreducer = "Jan 01 2021 00 error";
    private final String testmapper = "Jan 01 2021 00:00:18 3 kernel:message";
    private final String testmapperandreducer = "Jan 01 2021 00:00:18 3 kernel:message";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    /* test mapper*/
    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testmapper))
                .withOutput(new Text(testreducer), new IntWritable(1))
                .runTest();
    }
    /* test reducer*/
    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text(testreducer), values)
                .withOutput(new Text(testreducer), new IntWritable(2))
                .runTest();
    }
    /* combo test mapper+reducer*/
    @Test
    public void testMapReduce() throws IOException {
        IntWritable result = new IntWritable(1);
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testmapperandreducer))
                .withOutput(new Text(testreducer), result)
                .runTest();
    }
}
