package org.example;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TheMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    Text theKey = new Text(); // key
    private int key_idx; // key column index
    private int value_idx; // value column index

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // get parameters from context
        key_idx = context.getConfiguration().getInt("key_idx", 10);
        value_idx = context.getConfiguration().getInt("value_idx", 3);
        String line = value.toString();

        String[] lineArray = line.split("\t");
        String k = lineArray[key_idx];
        Float valueCount_f;

        try {
            valueCount_f = Float.parseFloat(lineArray[value_idx]);
        } catch (NumberFormatException e) {
            valueCount_f = 0f;
        }
        Integer valueCount_i = valueCount_f.intValue();
        DoubleWritable valueCount = new DoubleWritable(valueCount_i);
        theKey.set(k);
        context.write(theKey, valueCount);
    }
}