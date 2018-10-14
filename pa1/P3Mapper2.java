package com.bigdata.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class P3Mapper2 extends Mapper<Object, Text, IntWritable, Text> {
    IntWritable freq = new IntWritable();
    Text word = new Text();
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException{

        String[] val = value.toString().split("\t");
        int num = Integer.parseInt(val[1]);
        word.set(val[0]);
        freq.set(num);
        context.write(freq, word);
                }
            }
