package com.bigdata.pa1;

import com.bigdata.pa1.IntegerComparator;
import com.bigdata.pa1.Profile1Mapper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class Profile3 {

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[2]));
        FileUtils.deleteDirectory(new File(args[1]));
        if (args.length != 3) {
            System.err.println("Usage: Ngram <inputPath> <TempPath> <outputPath>");
            System.exit(2);
        }
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "p3-1");
        job1.setJarByClass(Profile3.class);
        job1.setNumReduceTasks(20);
        job1.setMapperClass(Profile1Mapper.class);
        job1.setCombinerClass(Profile3Reducer.class);
        job1.setReducerClass(Profile3Reducer.class);
//        job1.setMapOutputKeyClass(Text.class);
//        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // Second Job

        Job job2 = Job.getInstance(conf, "p3-2");
        job2.setJarByClass(Profile3.class);
        job2.setNumReduceTasks(6);
        job2.setSortComparatorClass(IntegerComparator.class);
        job2.setPartitionerClass(Profile3Partitioner.class);
        job2.setMapperClass(P3Mapper2.class);
        job2.setReducerClass(P3Reducer2.class);
//        job2.setCombinerClass(P3Reducer2.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputKeyClass(IntWritable.class);
        FileInputFormat.addInputPath(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true)? 0 : 1);

    }
}
