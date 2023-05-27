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
        String reducer = files[2];
        int key_idx = Integer.parseInt(files[3]);
        int value_idx = Integer.parseInt(files[4]);

        if (files.length != 5) {
            System.out.println("Usage: <input> <output> <reducer> <key_idx> <value_idx>");
            System.exit(1);
        }

        if (key_idx < 0 || value_idx < 0) {
            System.out.println("Invalid index");
            System.exit(1);
        }

        // add parameters in c
        c.setInt("key_idx", key_idx);
        c.setInt("value_idx", value_idx);

        Job j = new Job(c, "BigData");
        j.setJarByClass(Main.class);

        j.setMapperClass(TheMapper.class);

        if (reducer.equals("avg")) {
            j.setReducerClass(Average.AverageReducer.class);
        } else if (reducer.equals("count")) {
            j.setReducerClass(Count.CountReducer.class);
        } else if (reducer.equals("minmax")) {
            j.setReducerClass(MinMax.MinMaxReducer.class);
        } else if (reducer.equals("std")) {
            j.setReducerClass(StandardDeviation.StandardDeviationReducer.class);
        } else if (reducer.equals("sum")) {
            j.setReducerClass(Summation.SummationReducer.class);
        } else {
            System.out.println("Invalid reducer");
            System.exit(1);
        }

        // COMMON
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}
