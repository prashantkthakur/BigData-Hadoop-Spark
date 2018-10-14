package com.bigdata.pa1;

import com.bigdata.pa1.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hsqldb.lib.HashMap;

import java.io.IOException;
//import java.util.HashMap;
import java.util.StringTokenizer;

public class Profile2Mapper  extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();
    private final IntPair freq = new IntPair();
    private IntWritable one = new IntWritable(1);
    java.util.HashMap<String, Integer> combo = new java.util.HashMap<>();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (!value.toString().isEmpty()) {
            String id = value.toString().split("<====>")[1];
            StringTokenizer token = new StringTokenizer(value.toString().split("<====>")[2]);
            while (token.hasMoreTokens()) {
                String out = token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                if (!out.isEmpty()) {
                    String ckey = (id + "-" + out);
                    combo.putIfAbsent(ckey, 1);
                    if (combo.containsKey(ckey)) {
                        combo.put(ckey, combo.get(ckey) + 1);
//                    word.set(ckey);
//                    context.write(word,one);
                    }


                }

            }

        }
        System.out.println(combo.keySet());
//        for (String mykey : combo.keySet()) {
//            String[] keys = mykey.split("-");
//            IntPair mapKey = new IntPair(Integer.parseInt(keys[0]),combo.get(mykey));
//            word.set(keys[1]);
//            context.write(mapKey, word);
//        }
//    }
    }
}

