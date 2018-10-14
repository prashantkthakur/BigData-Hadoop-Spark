package com.bigdata.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class P3Reducer2 extends Reducer<IntWritable,Text, Text, IntWritable> {
    IntWritable freq = new IntWritable();
    Text word = new Text();
    public void reduce(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        freq.set(key.get());
        word.set(value);
        context.write(word,freq);
    }

}
