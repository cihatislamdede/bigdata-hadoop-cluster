package org.example;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Average {
    public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text theKey, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable value : values) {
                try {
                    sum += value.get();
                } catch (Exception e) {
                    sum += 0;
                }
                count += 1;
            }
            double average = sum / count;
            context.write(theKey, new DoubleWritable(average));
        }
    }
}

