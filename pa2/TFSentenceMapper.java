package com.bigdata.pa2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;

import java.io.*;
import java.util.*;

public class TFSentenceMapper extends Mapper<Object, Text, Text, Text> {

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
                        String[] splits = line.split("-");
                        String docIdWord = splits[0] + "-" + splits[4];
                        Double tfIdf = Double.parseDouble(splits[3]);
                        docIdToTFIDF.put(docIdWord, tfIdf);
                    }
                }

            }
        } catch (IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().isEmpty()) {
            String[] docSplits = value.toString().split("<====>");
            String docId = docSplits[1];
            String top3Sen="";
            TreeMap <Double, Integer> summary = new TreeMap <>();
            String[] sentences = docSplits[2].split("\\. ");
            for (String sent : sentences) {
                ArrayList <Double> sentencetfidf = new ArrayList <Double>();
                Double sum = 0.0;
                StringTokenizer article = new StringTokenizer(sent);
                while (article.hasMoreTokens()) {
                    String word = article.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                    if (!word.isEmpty()) {
                        String docIdWord = docId + "-" + word;
                        Double tfidfVal = docIdToTFIDF.get(docIdWord);
                        sentencetfidf.add(tfidfVal);
                    }

                    // Sort to get top 5 words in the sentence to compute the sentence TFIDF

                }
                Collections.sort(sentencetfidf, new Comparator <Double>() {
                    @Override
                    public int compare(Double c1, Double c2) {
                        return -Double.compare(c1, c2); // Return -ve for descending order
                    }
                });
                // Store the tfidf of each sentence
                if (sentencetfidf.size() > 5) {
                    for (Double val : sentencetfidf.subList(0, 5)) {
                        //                System.out.println(val);
                        sum += val;
                    }
                }
                int index = Arrays.asList(sentences).indexOf(sent);
                summary.put(sum, index);
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