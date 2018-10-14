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



public class Profile2 {

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            return IntPair.compare(ip1.getFirst(), ip2.getFirst());
        }
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: Ngram <inputPath> <tmpPath> <outputPath>");
            System.exit(2);
        }
        FileUtils.deleteDirectory(new File(args[1]));
//        FileUtils.deleteDirectory(new File(args[1]));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Profile2");
        job.setJarByClass(Profile2.class);
//        job.setNumReduceTasks(20);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
////        job.setPartitionerClass(Profile1Partitioner.class);
////        InputSampler.Sampler<Text,IntWritable> sampler = new InputSampler.RandomSampler<Text, IntWritable>(0.1,10000,10);
////        InputSampler.writePartitionFile(job, sampler);
////        job.setPartitionerClass(FirstPartitioner.class);
////        job.setSortComparatorClass(NgramComparator.class);
////        job.setGroupingComparatorClass(GroupComparator.class);
//
        job.setMapperClass(Profile2Mapper.class);
        job.setReducerClass(Profile2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);

        Job finalJob = Job.getInstance(conf, "Final Job");
//        finalJob.setNumReduceTasks(100);
        finalJob.setJarByClass(Profile2.class);
        FileInputFormat.addInputPath(finalJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(finalJob, new Path(args[2]));
        finalJob.setMapperClass(SecondMapper.class);
        finalJob.setReducerClass(SecondReducer.class);
        finalJob.setPartitionerClass(FirstPartitioner.class);
        finalJob.setSortComparatorClass(NgramComparator.class);
//            finalJob.setGroupingComparatorClass(GroupComparator.class);
        finalJob.setMapOutputKeyClass(IntPair.class);
        finalJob.setMapOutputValueClass(Text.class);
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(IntWritable.class);
        System.exit(finalJob.waitForCompletion(true) ? 0 : 1);

        }
    }
