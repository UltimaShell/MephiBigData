package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
/*Mapper: составляет ключ(кусок строки от начала по час + ошибка), значение для счетчика в редьюсере(1)*/
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final String RFC = "^[A-Z]{1}[a-z]{2} [0-9]{2} [0-9]{4} [0-9]{2}[:]{1}[0-9]{2}[:]{1}[0-9]{2} [0-9]{1} [a-z]{6}[:]{1}.*$";
    Pattern pattern1900 = Pattern.compile(RFC);
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    private final Text keyWritable = new Text();
    private static final String[] values_key = {"panic", "alert", "crit", "error", "warning", "notice", "info", "debug"};

    /**
     * @param key     Input key
     * @param value   String wrapper (Text hadoop) of input string
     * @param context Context of map-reduce job
     *                примерно: Jan 01 2021 00 alert 1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcher = pattern1900.matcher(value.toString());
        if (matcher.find()) {
            String[] ml = line.split(":| ");
            if (!ml[0].isEmpty() && !ml[ml.length - 1].isEmpty()){
                keyWritable.set(new Text(values_key[Integer.parseInt((ml[6]))]));
                word.set(ml[0]+" "+ml[1]+" "+ml[2]+" "+ml[3]+" "+keyWritable);
                context.write(word, one);
            }
        }else{
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}