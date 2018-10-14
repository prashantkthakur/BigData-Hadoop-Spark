package com.bigdata.pa2;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobClient;

import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapred.RunningJob;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class pa2

{

    public static enum DOCUMENT_COUNT{

        NoOfDocuments

    };

    public static class CountWords

            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();

        private Text DocId = new Text();

        public void map(Object key, Text value, Context context

        ) throws IOException, InterruptedException {

            String[] a=value.toString().split("<====>");

            if (!value.toString().isEmpty() && value!=null && value.toString().length()!=0 && a.length==3) {

                DocId.set(a[1]);

                StringTokenizer token = new StringTokenizer(a[2]);

                while (token.hasMoreTokens()) {

                    String out = token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();

                    if (out != null && !out.isEmpty()) {

                        word.set(DocId + "\t" + out);

                        context.write(word, one);

                    }

                }

            }

        }

    }

    public static class UnigramPartitioner extends Partitioner<Text,IntWritable>{

        @Override

        public int getPartition(Text key, IntWritable value, int numReduceTasks){

// TODO Auto-generated method stub

            if (numReduceTasks == 20){

                Integer DocId=Integer.parseInt(key.toString().split("\t")[0]);

                if (key==null||key.getLength()==0){

                    return 0;

                }

                return (DocId.hashCode()&Integer.MAX_VALUE)%numReduceTasks;

            }

            return 0;

        }

    }

    public static class CountWordReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,

                           Context context

        ) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {

                sum += val.get();

            }

            result.set(sum);

            context.write(key,result);

        }

    }

    public static class tfMapper

            extends Mapper<Object, Text, Text, Text>{

        private String word = new String();

        private Text DocId =new Text();

        private String Count=new String();

        private Text WordCount=new Text();

        public void map(Object key, Text value, Context context

        ) throws IOException, InterruptedException {

            if (!value.toString().isEmpty()) {

                DocId.set(value.toString().split("\t")[0]);

                word=value.toString().split("\t")[1];

                Count=value.toString().split("\t")[2];

                WordCount.set(word+"\t"+Count);

                context.write(DocId, WordCount);

            }

        }

    }

    public static class TFPartitioner extends Partitioner<Text,Text>{

        @Override

        public int getPartition(Text key, Text value, int numReduceTasks){

// TODO Auto-generated method stub

            if (numReduceTasks == 20){

                Integer DocId=Integer.parseInt(key.toString());

                if (key==null||key.getLength()==0){

                    return 0;

                }

                return (DocId.hashCode()&Integer.MAX_VALUE)%numReduceTasks;

            }

            return 0;

        }

    }

    public static class tfReducer extends Reducer<Text,Text,Text,Text>{

        double termFreq;

        Text tfValues=new Text();

        public void reduce(Text key, Iterable<Text> values,

                           Context context

        ) throws IOException, InterruptedException {

            ArrayList<String> WordCountList= new ArrayList<String>();

            double maxFreq = 0;

            for(Text val : values){

                WordCountList.add(val.toString());

                double count=Double.parseDouble(val.toString().split("\t")[1]);

                if(count>maxFreq){

                    maxFreq=count;

                }

            }

            for (String WordCount : WordCountList){

                String[] valueSplit=WordCount.split("\t");

                Double fi=Double.parseDouble(valueSplit[1]);

                termFreq=0.5+0.5*(fi/maxFreq);

                tfValues.set(valueSplit[0]+"\t"+termFreq);

                context.write(key,tfValues);

            }

        }

    }

    public static class idfMapper

            extends Mapper<Object, Text, Text, Text> {

        private String DocId = new String();

        private Text word = new Text();

        private String tf = new String();

        private Text NewVal = new Text();

        private ArrayList<String> DocCount=new ArrayList<>();

        public void map(Object key, Text value, Context context

        ) throws IOException, InterruptedException {

            if (!value.toString().isEmpty()) {

                DocId = value.toString().split("\t")[0];

                if(!DocCount.contains(DocId)) {

                    DocCount.add(DocId);

                    context.getCounter(DOCUMENT_COUNT.NoOfDocuments).increment(1);

                }

                word.set(value.toString().split("\t")[1]);

                tf = new String(value.toString().split("\t")[2]);

                NewVal.set(DocId + "\t" + word +"\t"+tf);

                context.write(word, NewVal);

            }

        }

    }

    public static class idfPartitioner extends Partitioner<Text,Text>{

        @Override

        public int getPartition(Text key, Text value, int numReduceTasks){

// TODO Auto-generated method stub

            if (numReduceTasks == 9){

                Character partitionKey=null;

                if (key==null||key.getLength()==0){

                    partitionKey='a';}

                else{

                    partitionKey=key.toString().toLowerCase().charAt(0);

                }

                if (partitionKey<='c' || Character.isDigit(partitionKey)){

                    return 0;

                }

                else if(partitionKey>'c' && partitionKey<='f'){

                    return 1;

                }

                else if (partitionKey>'f' && partitionKey<='i'){

                    return 2;}

                else if (partitionKey>'i' && partitionKey<='l'){

                    return 3;

                }

                else if (partitionKey>'l' && partitionKey<='o'){

                    return 4;}

                else if (partitionKey>'o' && partitionKey<='r'){

                    return 5;

                }

                else if (partitionKey>'r' && partitionKey<='u'){

                    return 6;}

                else if (partitionKey>'u' && partitionKey<='x'){

                    return 7;}

                else if(partitionKey>'x'){

                    return 8;}

            }

            return 0;

        }

    }

    public static class idfReducer extends Reducer<Text, Text, Text, Text> {

        String word = new String();

        Double idf, tfIdf, tf,count;

        Text DocID=new Text();

        private long mapperCounter;

        @Override

        public void setup(Context context) throws IOException, InterruptedException{

            Configuration conf = context.getConfiguration();

            JobClient client = new JobClient((JobConf) conf);

            RunningJob parentJob =

                    client.getJob((org.apache.hadoop.mapred.JobID) JobID.forName( conf.get("mapred.job.id") ));

            mapperCounter = parentJob.getCounters().getCounter(DOCUMENT_COUNT.NoOfDocuments);

        }

        public void reduce(Text key, Iterable<Text> values,

                           Context context

        ) throws IOException, InterruptedException {

            HashSet<String> cache1= new HashSet<String>();

            for (Text val : values) {

                cache1.add(val.toString());

            }

            for (String vali : cache1) {

                String[] valuesList = vali.toString().split("\t");

                DocID.set(valuesList[0].toString());

                tf = Double.parseDouble(valuesList[2]);

                word = valuesList[1].toString();

                idf = Math.log10(mapperCounter/ cache1.size());

                tfIdf = tf * idf;

                context.write(DocID, new Text(word+"\t"+tfIdf));

            }

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Word Count in Docuument");

        job1.setJarByClass(pa2.class);

        job1.setNumReduceTasks(20);

        System.out.println("Running job 1");

        job1.setMapperClass(CountWords.class);

        job1.setCombinerClass(CountWordReducer.class);

        job1.setPartitionerClass(UnigramPartitioner.class);

        job1.setReducerClass(CountWordReducer.class);

        job1.setOutputKeyClass(Text.class);

        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));

        FileOutputFormat.setOutputPath(job1,new Path(args[1]));

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Calculate tf");

        job2.setJarByClass(pa2.class);

        job2.setNumReduceTasks(20);

        job2.setMapperClass(tfMapper.class);

        job2.setCombinerClass(tfReducer.class);

        job2.setPartitionerClass(TFPartitioner.class);

        job2.setReducerClass(tfReducer.class);

        job2.setOutputKeyClass(Text.class);

        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]+"/*"));

        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);

        System.out.println("Running job3 ");

        Job job3 = Job.getInstance(conf, "Calculate IDF and TF-IDF");

        job3.setJarByClass(pa2.class);

        job3.setNumReduceTasks(9);

        job3.setMapperClass(idfMapper.class);

        job3.setPartitionerClass(idfPartitioner.class);

        job3.setReducerClass(idfReducer.class);

        job3.setOutputKeyClass(Text.class);

        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[2] + "/*"));

        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        System.exit(job3.waitForCompletion(true)?0:1);

    }

}