package com.bigdata.pa2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadDatasetMapper extends Mapper <LongWritable, Text,IntWritable,Text> {

    Text valOut = new Text();
    IntWritable keyOut = new IntWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        if (!value.toString().isEmpty()) {
            String[] splits = value.toString().split("<====>");
            if (splits.length >2) {
                Integer docId = Integer.parseInt(splits[1]);
                keyOut.set(docId);
                valOut.set("S="+splits[2]);
            }
        }
    }
}
