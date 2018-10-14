package com.bigdata.pa2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SentenceReducer extends Reducer<IntWritable,Text, IntWritable, Text> {

    private Text value = new Text();
    private Text outVal = new Text();
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String top3Sen = "";
        HashMap<String, Double> arrayTFIDF = new HashMap<>();

        while (values.iterator().hasNext()) {
            value = values.iterator().next();
            if (value.toString().substring(0, 2) == "S=") {
                TreeMap<Double, Integer> summary = new TreeMap<>();
                String[] sentences = value.toString().substring(2).split("\\. ");
                for (String sent : sentences) {
                    ArrayList<Double> sentencetfidf = new ArrayList<Double>();
                    Double sum = 0.0;
                    StringTokenizer article = new StringTokenizer(sent);
                    while (article.hasMoreTokens()) {
                        String word = article.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                        if (!word.isEmpty()) {
                            String docIdWord = word;
                            Double tfidfVal = arrayTFIDF.get(docIdWord);
                            sentencetfidf.add(tfidfVal);
                        }

                        // Sort to get top 5 words in the sentence to compute the sentence TFIDF

                    }
                    Collections.sort(sentencetfidf, new Comparator<Double>() {
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
                ArrayList<Integer> topSen = new ArrayList<Integer>();
                for (Integer val : summary.descendingMap().values()) {
                    if (i > 2) break;
                    topSen.add(val);
                    i++;
                }
                Collections.sort(topSen, new Comparator<Integer>() {
                    @Override
                    public int compare(Integer c1, Integer c2) {
                        return Integer.compare(c1, c2);
                    }
                });
                for (Integer val : topSen) {
                    top3Sen += sentences[val] + ". ";
                }

            }
            if (value.toString().substring(0,2)=="T="){
                String[] splits = values.toString().substring(2).split("=");
                arrayTFIDF.put()
            }
            outVal.set(top3Sen);
            context.write(key, outVal);
        }
    }
}
