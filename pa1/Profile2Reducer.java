package com.bigdata.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Profile2Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
//                System.out.println(key + ":: "+sum);
                result.set(sum);
            }
            result.set(sum);
            Text out = new Text(key.toString()+"-"+sum);
        context.write(out, null);

    }
}