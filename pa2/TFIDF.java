package com.bigdata.pa2;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;

//enum DOCUMENT_COUNT{NoOfDocuments}
enum DocCount{CNT};
public class TFIDF {
    public static enum DOCUMENT_COUNT{

        NoOfDocuments

    };
//    public static enum DocCount{CNT}
    public static long docCnt;
    public static void main(String[] args) throws Exception {
//        FileUtils.deleteDirectory(new File(args[1]));
//        FileUtils.deleteDirectory(new File(args[2]));
//        FileUtils.deleteDirectory(new File(args[4]));
        if (args.length != 5) {
            System.err.println("Usage: hadoop jar <jarFile> com.bigdata.pa2.TFIDF <inputPath> <TempPath1>" +
                    " <TempPath2> <outputPath>");
            System.exit(2);
        }

        // Start first job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "tfidf-1");
        job1.setJarByClass(TFIDF.class);
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
//        docCnt = job1.getCounters().findCounter(DocCount.CNT).getValue();
        Counters someCount = job1.getCounters();
        Counter counter = someCount.findCounter(DocCount.CNT);
//        System.out.println("EEEEEE:"+counter.getValue());
//        PrintWriter writer = new PrintWriter("DocCount.txt", "UTF-8");
//        writer.print(docCnt);
//        writer.close();


        // Second Job
        Job job2 = Job.getInstance(conf, "tfidf-2");
        job2.getConfiguration().setLong(DocCount.CNT.name(), counter.getValue());
        job2.setJarByClass(TFIDF.class);
        job2.setMapperClass(IDFMapper.class);
        job2.setReducerClass(IDFReducer.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
//

        //Third job - Start sentence manipulation
//        DistributedCache.addCacheFile(new URI("tmp2/part-r-00000"), conf);
//        Job job3 = Job.getInstance(conf, "tfidf-sentence");
//        job3.setJarByClass(TFIDF.class);
//        job3.setMapperClass(TFSentenceMapper.class);
//        job3.setNumReduceTasks(0);
//        job3.setOutputKeyClass(Text.class);
//        job3.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job3, new Path(args[3]));
//        FileOutputFormat.setOutputPath(job3, new Path(args[4]));
//        System.exit(job3.waitForCompletion(true)? 0 : 1);

    }
}
