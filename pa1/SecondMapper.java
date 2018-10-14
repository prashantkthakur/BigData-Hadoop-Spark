package com.bigdata.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SecondMapper extends Mapper<LongWritable, Text, IntPair, Text>  {
    private Text out = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        if (!value.toString().isEmpty()) {
            String[] content = value.toString().split("-");
            Integer count = Integer.parseInt(content[2]);
            Integer id = Integer.parseInt(content[0]);
            String word = content[1];
            out.set(word);
            IntPair mapKey = new IntPair(id,count);
//            System.out.println(mapKey);
            context.write(mapKey, out);
                }
            }
}

