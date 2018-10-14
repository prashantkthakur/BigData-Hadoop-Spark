package com.bigdata.pa2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class IDFMapper extends Mapper<LongWritable, Text,Text, Text> {
    private Text outVal = new Text();
    private Text outKey = new Text();
    private String docId = new String();
    private int id;
    private int lastSeen= 0;
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // Start edit for incrementing counter in this mapper
        docId = value.toString().split("\t")[0];
        id = Integer.parseInt(docId);
        if (id!=lastSeen) {
            lastSeen = id;
            context.getCounter(TFIDF.DOCUMENT_COUNT.NoOfDocuments).increment(1);

        }
        // End of edit

        String[] splitValues = value.toString().split("\t");
        outKey.set(splitValues[3]);
        outVal.set(splitValues[0]+ "-" + splitValues[1] + "-" + splitValues[2]);
        context.write(outKey,outVal);
    }
}
