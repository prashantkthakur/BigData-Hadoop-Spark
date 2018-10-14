package com.bigdata.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class Profile1Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {

//    public class Profile1Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            context.write(key, null);
        }
    }
//}