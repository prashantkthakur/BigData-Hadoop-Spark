package com.bigdata.pa1;

import java.io.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;



public class Profile1 {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: Ngram <inputPath> <outputPath>");
            System.exit(2);
        }
        FileUtils.deleteDirectory(new File(args[1]));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Ngram-Profile1");
        job.setJarByClass(Profile1.class);
        job.setNumReduceTasks(5);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        job.setPartitionerClass(Profile1Partitioner.class);
//        InputSampler.Sampler<Text,IntWritable> sampler = new InputSampler.RandomSampler<Text, IntWritable>(0.1,10000,10);
//        InputSampler.writePartitionFile(job, sampler);
//        job.setPartitionerClass(FirstPartitioner.class);
//        job.setSortComparatorClass(NgramComparator.class);
//        job.setGroupingComparatorClass(GroupComparator.class);

        job.setMapperClass(Profile1Mapper.class);
        job.setReducerClass(Profile1Reducer.class);
//        job.setReducerClass(SecondReducer.class);
        job.setPartitionerClass(Profile1Partitioner.class);
//        job.setSortComparatorClass(NgramComparator.class);
//        job.setGroupingComparatorClass(GroupComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        }
    }