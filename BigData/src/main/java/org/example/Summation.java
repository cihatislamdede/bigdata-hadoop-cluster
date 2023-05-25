package org.example;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Summation {
    public static class SummationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text year, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double sum = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(year, new DoubleWritable(sum));
        }
    }
}