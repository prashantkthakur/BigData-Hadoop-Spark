package com.bigdata.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Profile3Partitioner extends Partitioner<IntWritable, Text> {
    @Override
    public int getPartition(IntWritable key, Text value, int numReduceTasks) {
        if (numReduceTasks == 8) {
            if (key.get() > 10000) return 0;
            else if (key.get() > 100 && key.get() < 10000) return 1;
            else if (key.get() > 10 && key.get() < 100) return 2;
            else if (key.get() > 5 && key.get() < 10) return 3;
            else if (key.get() > 3 && key.get() < 5) return 4;
            else return 5;
        } else return (key.hashCode() % numReduceTasks);
    }
}
