package com.bigdata.pa2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class TFReducer extends Reducer<IntPair, Text, IntPair, Text> {
    private IntWritable reducerKey = new IntWritable();
    private Double tf;
    private double lastSeen;
    private double maxFreq;
    Date date = new Date();
    private FileWriter fw;
    // Each IntPair would be represented and come in as key and value in iterables for matching IntPair key.
    public void reduce(IntPair key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
        Integer first = key.getFirst();
        Integer second = key.getSecond();
        if (first != lastSeen) {
            lastSeen = first;
            maxFreq = second;
        }
        for (Text val : values) {
            String[] inputVal = val.toString().split("\t");
            tf = 0.5 + 0.5 * ((double) Integer.parseInt(inputVal[0]) / maxFreq);
//        Text outVal = new Text(tf.toString() + "\t" + inputVal[1]+"|||||||"+maxFreq+"lastseen::"+lastSeen+"First::"+first+"=>"+(first !=lastSeen));
            Text outVal = new Text(tf.toString() + "\t" + inputVal[1]);
            context.write(key, outVal);
//            System.out.println("REDDDDDDDDDDDDDDDDDDDDD:"+context.getCounter(DocCount.CNT).getValue());

        }
    }
}

