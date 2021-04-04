package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
/**
 * Редьюсер: считает количество ошибок определённого типа за час.
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * @param key Text from mapper in format Mmm dd yyyy hh name-of-error
     * @param values Quantity of keys (если повезет то из Combiner ну или нет )
     * @param context Context of map-reduce job
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
        }
        context.write(key, new IntWritable(sum));
    }
}