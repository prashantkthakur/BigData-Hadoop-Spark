package com.bigdata.pa2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TFIDFReaderMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    IntWritable outKey = new IntWritable();
    Text outVal = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] splits = value.toString().split("-");
        Integer docId = Integer.parseInt(splits[0]);
        String out = "T="+splits[2]+"="+splits[1];
        outKey.set(docId);
        outVal.set(out);
        context.write(outKey,outVal);

    }
}
