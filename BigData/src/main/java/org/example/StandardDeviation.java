package org.example;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StandardDeviation {
    public static class StandardDeviationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text year, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double sum = 0;
            double sumSquare = 0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                sumSquare += value.get() * value.get();
                count+=1;
            }
            double mean = sum / count;
            double variance = sumSquare / count - mean * mean;
            double stdDev = Math.sqrt(variance);
            context.write(year, new DoubleWritable(stdDev));
        }
    }
}

