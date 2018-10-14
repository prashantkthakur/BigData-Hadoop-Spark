package com.bigdata.pa2;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import java.io.*;
import java.net.URI;
import java.util.*;

public class MyPa2 {
    static String docCount;
    public static class NgramComparator extends WritableComparator {

        protected NgramComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
            if (cmp != 0) {
                return cmp;
            }
            return -IntPair.compare(ip1.getSecond(), ip2.getSecond());//reverse
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, Text> {
        @Override
        public int getPartition(IntPair key, Text value, int numPartitions) {
            // multiply by 127 to perform some mixing
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    public static class IntPair implements WritableComparable<IntPair> {

        private int first;
        private int second;

        public IntPair() {
        }

        public IntPair(int first, int second) {
            set(first, second);
        }

        public void set(int first, int second) {
            this.first = first;
            this.second = second;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first);
            out.writeInt(second);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readInt();
            second = in.readInt();
        }

        @Override
        public int hashCode() {
            return first * 163 + second;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof IntPair) {
                IntPair ip = (IntPair) o;
                return first == ip.first && second == ip.second;
            }
            return false;
        }

        @Override
        public String toString() {
            return first + "\t" + second;
        }

        @Override
        public int compareTo(IntPair ip) {
            int cmp = compare(first, ip.first);
            if (cmp != 0) {
                return cmp;
            }
            return compare(second, ip.second);
        }

        /**
         * Convenience method for comparing two ints.
         */
        public static int compare(int a, int b) {
            return (a < b ? -1 : (a == b ? 0 : 1));
        }
    }

    public static class TFMapper extends Mapper<Object, Text, IntPair, Text> {
        Text outVal = new Text();
        Map<String, Integer> countMap = new HashMap<String, Integer>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
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
                    Iterator<Map.Entry <String, Integer>> iterator = countMap.entrySet().iterator();
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

    public static class TFReducer extends Reducer<IntPair, Text, IntPair, Text> {
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
                Text outVal = new Text(tf.toString() + "\t" + inputVal[1]);
                context.write(key, outVal);

            }
        }
    }
    public static class CountDocIdMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
        private Text outKey = new Text();
        private String docId = new String();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                docId = value.toString().split("<====>")[1];

                outKey.set(docId);
                context.write(outKey, NullWritable.get());
            }
        }
    }

    public static class CountDocIdReducer extends Reducer<Text, NullWritable, LongWritable, NullWritable> {
        long val;
        public void reduce(Text key, Iterable <NullWritable> values, Context context) throws IOException, InterruptedException {
             val = val + 1;
            context.write(new LongWritable(val),NullWritable.get());
        }


    }

    public static class IDFMapper extends Mapper<LongWritable, Text,Text, Text> {
        private Text outVal = new Text();
        private Text outKey = new Text();
        private String docId = new String();
        private int id;
        private int lastSeen= 0;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            // Start edit for incrementing counter in this mapper
//            System.out.println(value.toString());
//            docId = value.toString().split("\t")[0];
//            id = Integer.parseInt(docId);
//            if (id!=lastSeen) {
//                lastSeen = id;
//                context.getCounter(DOCUMENT_COUNT.NoOfDocuments).increment(1);
//
//            }
            // End of edit

            String[] splitValues = value.toString().split("\t");
            outKey.set(splitValues[3]);
            outVal.set(splitValues[0]+ "=" + splitValues[1] + "=" + splitValues[2]);
            context.write(outKey,outVal);
        }
    }

    public static class IDFReducer extends Reducer<Text, Text, Text, NullWritable> {

        // Start of of someCount implementation.

//        private long someCount;
//
//        @Override
//        protected void setup(Context context) throws IOException,
//                InterruptedException {
//            super.setup(context);
//            this.someCount  = context.getConfiguration().getLong(DocCount.CNT.name(),0);
//        }
        // End of someCount implementation


        //Mod1
//        private long mapperCounter;
//        @Override
//        public void setup(Context context) throws IOException{
//
//            Configuration conf = context.getConfiguration();
//
//            JobClient client = new JobClient((JobConf) conf);
//
//            RunningJob parentJob =client.getJob((org.apache.hadoop.mapred.JobID) JobID.forName( conf.get("mapred.job.id")));
//
//            mapperCounter = parentJob.getCounters().getCounter(DOCUMENT_COUNT.NoOfDocuments);
//
//        }
//        End Mod1

        //   Read from file the DocCounter value.
        //
        //    {
        //        try {
        //            content = FileUtils.readFileToString(new File("DocCount.txt"), "UTF-8");
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //        }
        //    }
        //    private long docCount = Long.parseLong(content);

        //End of read
        String last;
        @Override
        public void setup(Context context) throws IOException{
            Path pt=new Path("./count/part-r-00000");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader inputLocal = new BufferedReader(new InputStreamReader(fs.open(pt)));
//            BufferedReader inputLocal = new BufferedReader(new FileReader("./count/part-r-00000"));
            String line;

            while((line =inputLocal.readLine())!=null) {
            last = line;
            }
        }

        private Text outKey = new Text();
        private Text outVal = new Text();
//        Long N = Long.parseLong(last);

        public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {

            HashSet <String> inputList = new HashSet<String>();
            long docCount;
            long cnt=0;
//        docCount = context.getCounter(DocCount.CNT).getValue();
            Double idf=0.0;
            for (Text val : values) {
//                System.out.println(val.toString());
                inputList.add(val.toString());
            }
            cnt = inputList.size();
            for (String input: inputList){
                String[] data = input.split("=");
                idf = Math.log10(Long.parseLong(last)/cnt);
                outKey.set(input+"="+idf*Double.parseDouble(data[2])+"="+key);
                context.write(outKey, NullWritable.get());
            }
//        System.out.println("COUNT:"+cnt+"Key:"+key+"cont::"+someCount);

        }
    }

    public static class TFSentenceMapper extends Mapper<Object, Text, Text, Text> {

        // Load content from distributed cache into hashmap.
        private HashMap <String, Double> docIdToTFIDF = new HashMap <String, Double>();
        private Text outVal = new Text();
        private Text outKey = new Text();

        public void setup(Context context) throws IOException, InterruptedException {
            /*
             * Stores DocumentId-word in a hashmap as key and TFIDF of the corresponding word as value
             */
            try {
                Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (files != null && files.length > 0) {
                    for (Path p : files) {
                        BufferedReader rdr = new BufferedReader(new FileReader(p.toString()));
                        String line = null;
                        while ((line = rdr.readLine()) != null) {
                            String[] splits = line.split("=");
                            String docIdWord = splits[0] + "=" + splits[4];
                            Double tfIdf = Double.parseDouble(splits[3]);
                            docIdToTFIDF.put(docIdWord, tfIdf);
                        }
                    }

                }
            } catch (IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
            }
        }

        public void map(Object keys, Text inputval, Context context) throws IOException, InterruptedException {
            if (!inputval.toString().isEmpty()) {
                String[] docSplits = inputval.toString().split("<====>");
                String docId = docSplits[1];
                String top3Sen="";
                TreeMap <Double, Integer> summary = new TreeMap <>();
                String[] sentences = docSplits[2].split("\\. ");
                for (String sent : sentences) {
//                    ArrayList <Double> sentencetfidf = new ArrayList <Double>();
                    TreeMap <Double,Double> sentencetfidf = new TreeMap <>();
                    Double sum = 0.0;
                    StringTokenizer article = new StringTokenizer(sent);
                    while (article.hasMoreTokens()) {
                        String word = article.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                        if (!word.isEmpty()) {
                            String docIdWord = docId + "=" + word;
                            Double tfidfVal = docIdToTFIDF.get(docIdWord);
                            sentencetfidf.put(tfidfVal,tfidfVal);
                        }

                        // Sort to get top 5 words in the sentence to compute the sentence TFIDF

                    }
                    System.out.println(sentencetfidf);
//                    Collections.sort(sentencetfidf, new Comparator <Double>() {
//                        @Override
//                        public int compare(Double c1, Double c2) {
//                            return -Double.compare(c1, c2); // Return -ve for descending order
//                        }
//                    });

                    // Store the tfidf of each sentence
//                    if (sentencetfidf.size() > 5) {
//                        for (Double val : sentencetfidf.subList(0, 5)) {
//                            //                System.out.println(val);
//                            sum += val;
//                        }
//                    }

                    int i=0;
                    Double tfidfsum=0.0;
                    for (Double val: sentencetfidf.descendingMap().values()){
                        if (i <3){
                            tfidfsum +=val;
                        }
                        i++;
                    }


                    int index = Arrays.asList(sentences).indexOf(sent);
                    summary.put(tfidfsum, index);
                }
                int i = 0;
                ArrayList <Integer> values = new ArrayList <Integer>();
                for (Integer val : summary.descendingMap().values()){
                    if (i > 2) break;
                    values.add(val);
                    i++;
                }
                Collections.sort(values, new Comparator <Integer>() {
                    @Override
                    public int compare(Integer c1, Integer c2) {
                        return Integer.compare(c1, c2);
                    }
                });
                for (Integer val: values){
                    top3Sen += sentences[val]+". ";
                }
                outKey.set(docId);
                outVal.set(top3Sen);
                context.write(outKey, outVal);
            }
        }
    }


// Driver class

    public enum DOCUMENT_COUNT{NoOfDocuments};
    //    public static enum DocCount{CNT}
    public static long docCnt;
    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[1]));
        FileUtils.deleteDirectory(new File(args[2]));
//        FileUtils.deleteDirectory(new File(args[3]));
//        if (args.length != 5) {
//            System.err.println("Usage: hadoop jar <jarFile> com.bigdata.pa2.TFIDF <inputPath> <TempPath1>" +
//                    " <TempPath2> <outputPath>");
//            System.exit(2);
//        }

        // Start first job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "tfidf-1");
        job1.setJarByClass(MyPa2.class);
        job1.setNumReduceTasks(20);
        job1.setMapperClass(TFMapper.class);
        job1.setSortComparatorClass(NgramComparator.class);
        job1.setPartitionerClass(FirstPartitioner.class);
        job1.setReducerClass(TFReducer.class);
        job1.setMapOutputKeyClass(IntPair.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntPair.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // second job test.
        Job job4 = Job.getInstance(conf, "tfidf-2");
        job4.getConfiguration().set("docCount","0");
        job4.setJarByClass(MyPa2.class);
        job4.setNumReduceTasks(1);
        job4.setMapperClass(CountDocIdMapper.class);
        job4.setReducerClass(CountDocIdReducer.class);
        job4.setOutputValueClass(NullWritable.class);
        job4.setOutputKeyClass(LongWritable.class);
        job4.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(args[0]));
        FileOutputFormat.setOutputPath(job4, new Path("./count"));
        job4.waitForCompletion(true);

        // Second Job
        Job job2 = Job.getInstance(conf, "tfidf-3");
        job2.setJarByClass(MyPa2.class);
        job2.setNumReduceTasks(20);
        job2.setMapperClass(IDFMapper.class);
        job2.setReducerClass(IDFReducer.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);


        //Third job - Start sentence manipulation
//        DistributedCache.addCacheFile(new URI(args[2]+"/part-r-00000"), conf);
//        Job job3 = Job.getInstance(conf, "tfidf-sentence");
//        job3.setJarByClass(MyPa2.class);
//        job3.setMapperClass(TFSentenceMapper.class);
//        job3.setNumReduceTasks(0);
//        job3.setOutputKeyClass(Text.class);
//        job3.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job3, new Path(args[3]));
//        FileOutputFormat.setOutputPath(job3, new Path(args[4]));
        System.exit(job2.waitForCompletion(true)? 0 : 1);
//        System.exit(job2.waitForCompletion(true)? 0 : 1);

    }
}

