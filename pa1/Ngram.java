package com.bigdata.pa1;
import java.io.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;



public class Ngram {

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

        if (args.length != 2) {
            System.err.println("Usage: Ngram <inputPath> <outputPath>");
            System.exit(2);
        }
        FileUtils.deleteDirectory(new File("./hdout"));
        FileUtils.deleteDirectory(new File("./tmp1"));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Ngram");
        job.setJarByClass(Ngram.class);
        job.setNumReduceTasks(5);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("./tmp1"));

//        job.setPartitionerClass(Profile1Partitioner.class);
//        InputSampler.Sampler<Text,IntWritable> sampler = new InputSampler.RandomSampler<Text, IntWritable>(0.1,10000,10);
//        InputSampler.writePartitionFile(job, sampler);
//        job.setPartitionerClass(FirstPartitioner.class);
//        job.setSortComparatorClass(NgramComparator.class);
//        job.setGroupingComparatorClass(GroupComparator.class);

        job.setMapperClass(Profile2Mapper.class);
//        job.setMapperClass(SecondMapper.class);
        job.setReducerClass(Profile2Reducer.class);
//        job.setReducerClass(SecondReducer.class);
//        job.setSortComparatorClass(NgramComparator.class);
//        job.setGroupingComparatorClass(GroupComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
//        System.out.println("Success: "+success);
        if (success) {
            Configuration conf1 = new Configuration();
            Job finalJob = Job.getInstance(conf1, "Final Job");
            finalJob.setNumReduceTasks(5);
            finalJob.setJarByClass(Ngram.class);
            System.out.println("Second job");
            FileInputFormat.addInputPath(finalJob, new Path("./tmp1/part-r-00000"));
            FileOutputFormat.setOutputPath(finalJob, new Path(args[1]));
            finalJob.setMapperClass(SecondMapper.class);
            finalJob.setReducerClass(SecondReducer.class);
            finalJob.setPartitionerClass(FirstPartitioner.class);
            finalJob.setSortComparatorClass(NgramComparator.class);
            finalJob.setGroupingComparatorClass(GroupComparator.class);
            finalJob.setOutputKeyClass(Text.class);
            finalJob.setOutputValueClass(IntWritable.class);
            System.out.println(job.waitForCompletion(true) ? 0 : 1);
//
//        }
        }
    }
}
