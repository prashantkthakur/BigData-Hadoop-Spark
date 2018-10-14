package com.bigdata.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class NgramMapper extends Mapper <Object, Text, Text, IntWritable>{

//    public class Profile1Mapper extends Mapper <Object, Text, Text, IntWritable>{
        private Text word = new Text();
        private final  IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException{
            if (!value.toString().isEmpty()) {
                StringTokenizer token = new StringTokenizer(value.toString().split("<====>")[2]);
                while (token.hasMoreTokens()) {
                    String out = token.nextToken().replaceAll("[^A-Za-z0-9]","").toLowerCase();
                    if (!out.isEmpty()) {
                        word.set(out);
                        context.write(word, one);
                    }
                }
            }

        }
    }

//}
