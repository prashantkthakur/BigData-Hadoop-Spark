package com.bigdata.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondReducer extends Reducer<IntPair, Text, Text, IntWritable> {

    private Text result = new Text();
    public void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {

        Text id = new Text();
        IntWritable freq = new IntWritable();
        for (Text val: values){
            result.set(val);
            Integer ids = key.getFirst();
            id.set(ids.toString());
            freq.set(key.getSecond());
//            System.out.println(key);
            Text mykey = new Text(id.toString()+"\t"+result);
            context.write(mykey, freq);
        }


    }

}