package org.example;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMax {

    public static class MinMaxMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text theKey = new Text(); 		// key
        private final int key_idx = 10; 	// key column index
        private final int value_idx = 3;// value column index

        private String[] listLine(String row) {

            String[] lineList;
            row = row.substring(0, row.length() - 1);
            lineList = row.split("\t");
            return lineList;
        }


        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] lineInArray = listLine(value.toString());
            String k = lineInArray[key_idx];
            Float valueCount_f;

            try{
                valueCount_f = Float.parseFloat(lineInArray[value_idx]);
            }
            catch(NumberFormatException e){
                valueCount_f = 0f;
            }
            Integer valueCount_i = valueCount_f.intValue();
            DoubleWritable valueCount = new DoubleWritable(valueCount_i);
            theKey.set(k);
            context.write(theKey, valueCount);
        }
    }

    public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text year, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double min = 999999999999d;
            double max = 0;
            double temp;
            for (DoubleWritable val : values) {
                temp = val.get();
                if (temp > max)
                    max = temp;
                if (temp < min)
                    min = temp;
            }
            context.write(year, new DoubleWritable(min));
            context.write(year, new DoubleWritable(max));
        }
    }
}
