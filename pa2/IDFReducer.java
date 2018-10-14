package com.bigdata.pa2;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import java.util.HashSet;


public class IDFReducer extends Reducer<Text, Text, Text, NullWritable>{

    private long someCount;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        this.someCount  = context.getConfiguration().getLong(DocCount.CNT.name(),0);
    }
//
    //Mod1
//  private long mapperCounter;
//    @Override
//
//    public void setup(Context context) throws IOException{
//
//        Configuration conf = context.getConfiguration();
//
//        JobClient client = new JobClient((JobConf) conf);
//
//        RunningJob parentJob =client.getJob(JobID.forName( conf.get("mapred.job.id") ));
//
//        mapperCounter = parentJob.getCounters().getCounter(TFIDF.DOCUMENT_COUNT.NoOfDocuments);
//
//    }
    //End Mod1

//    String content;
//
//    {
//        try {
//            content = FileUtils.readFileToString(new File("DocCount.txt"), "UTF-8");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//    private long docCount = Long.parseLong(content);
    private Text outKey = new Text();
    private Text outVal = new Text();

    public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
        HashSet <String> inputList = new HashSet<String>();
        long docCount;
        long cnt=0;
//        docCount = context.getCounter(DocCount.CNT).getValue();
        Double idf=0.0;
        for (Text val : values) {
            inputList.add(val.toString());
        }
        cnt = inputList.size();
        for (String input: inputList){
            String[] data = input.split("-");
            idf = Math.log10(someCount/cnt);
            outKey.set(input+"-"+idf*Double.parseDouble(data[2])+"-"+key);
            context.write(outKey, NullWritable.get());
        }
//        System.out.println("COUNT:"+cnt+"Key:"+key+"cont::"+someCount);
//        System.out.println("CCCCC:"+context.getCounter(DocCount.CNT).getValue());

    }
}