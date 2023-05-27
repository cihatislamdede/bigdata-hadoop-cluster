package org.example;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Count {
    public static class CountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text year, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;

            for (DoubleWritable value : values) {
                count++;
            }
            context.write(year, new DoubleWritable(count));
        }
    }
}

