package org.example;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMax {
    public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text year, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double min = 999999999999d;
            double max = 0;
            double temp;
            for (DoubleWritable val : values) {
                try {
                    temp = val.get();
                    if (temp > max)
                        max = temp;
                    if (temp < min)
                        min = temp;
                } catch (Exception e) {
                }
            }
            context.write(year, new DoubleWritable(min));
            context.write(year, new DoubleWritable(max));
        }
    }
}
