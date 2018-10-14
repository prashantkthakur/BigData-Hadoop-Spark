package com.bigdata.pa2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountDocIdReducer extends Reducer<IntWritable,Text,Text,Text> {
    Integer uniqueId = 0;
    public void reduce(IntWritable key, Iterable <Text> values, Context context) throws IOException, InterruptedException{
        uniqueId ++;
    }
}
