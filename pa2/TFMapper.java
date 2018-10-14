package com.bigdata.pa2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class TFMapper extends Mapper<Object, Text, IntPair, Text> {


    Text outVal = new Text();
    Map<String, Integer> countMap = new HashMap<String, Integer>();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().isEmpty()) {
//            context.getCounter(DOCUMENT_COUNT.NoOfDocuments).increment(1);
            context.getCounter(DocCount.CNT).increment(1);
            String[] docSplits = value.toString().split("<====>");
            if (docSplits.length >2){
            String docId = docSplits[1];
            StringTokenizer article = new StringTokenizer(docSplits[2]);
            while (article.hasMoreTokens()) {
                String word = article.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                if (!word.isEmpty()) {
                    String mkey = docId + "-" + word;
                    countMap.computeIfPresent(mkey, (k, v) -> v + 1);
                    countMap.putIfAbsent(mkey, 1);
                }
            }
            Iterator <Map.Entry <String, Integer>> iterator = countMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry <String, Integer> entry = iterator.next();
                String[] mapKey = entry.getKey().split("-");
                Integer mapVal = entry.getValue();
                IntPair intkey = new IntPair(Integer.parseInt(mapKey[0]), mapVal);
//                System.out.printf("Key : %s and Value: %s %n", mapKey[0], mapVal);
                outVal.set(mapVal + "\t" + mapKey[1]);
                iterator.remove();
                context.write(intkey, outVal);

            }
        }
    }

    }
}