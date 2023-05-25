package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
    public static void main(String[] args) throws Exception {

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j = new Job(c, "Main");
        j.setJarByClass(Main.class);
        j.setMapperClass(TheMapper.class);

        // Average
        //j.setReducerClass(Average.AverageReducer.class);

        // Count
        //j.setReducerClass(Count.CountReducer.class);

        // Min-Max
        //j.setReducerClass(MinMax.MinMaxReducer.class);

        // Standard Deviation
        j.setReducerClass(StandardDeviation.StandardDeviationReducer.class);

        // Summation
        //j.setReducerClass(Summation.SummationReducer.class);

        // COMMON
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}
