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

        // Summation
        /*Job j = new Job(c, "summation");
        j.setJarByClass(Summation.class);
        j.setMapperClass(Summation.SummationMapper.class);
        j.setReducerClass(Summation.SummationReducer.class);
*/

        // Min-Max
        @SuppressWarnings("deprecation")
        /*Job j = new Job(c, "minmax");
        j.setJarByClass(MinMax.class);
        j.setMapperClass(MinMax.MinMaxMapper.class);
        j.setReducerClass(MinMax.MinMaxReducer.class);
*/

        // Average
        /*Job j = new Job(c, "average");
        j.setJarByClass(Average.class);
        j.setMapperClass(Average.AverageMapper.class);
        j.setReducerClass(Average.AverageReducer.class);
*/

        // Count
        /*Job j = new Job(c, "count");
        j.setJarByClass(Count.class);
        j.setMapperClass(Count.CountMapper.class);
        j.setReducerClass(Count.CountReducer.class);*/


        // Standard Deviation
        Job j = new Job(c, "standardDeviation");
        j.setJarByClass(StandardDeviation.class);
        j.setMapperClass(StandardDeviation.StandardDeviationMapper.class);
        j.setReducerClass(StandardDeviation.StandardDeviationReducer.class);


        // COMMON
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}
