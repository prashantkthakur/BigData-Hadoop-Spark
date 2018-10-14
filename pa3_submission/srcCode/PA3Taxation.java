import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class PA3Taxation {
//    private static final Pattern SPACES = Pattern.compile(":\\s+");
//    private static class Sum implements Function2<Double, Double, Double> {
//        public Double call(Double a, Double b) {
//            return a + b;
//        }
//    }
//    public static void main(String[] args) throws Exception {
//        Integer ITERATIONS = 25;
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("akka").setLevel(Level.OFF);
//        // Create Spark session
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaPageRank")
//                .config("spark.master", "local[1]")
//                .getOrCreate();
//        spark.conf().set("spark.driver.maxResultSize", "3g");
//        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
////        System.out.println(lines.collect());
//        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
//            String[] parts = SPACES.split(s);
//            return new Tuple2<>(parts[0], parts[1]);
//        }).distinct().groupByKey().cache();
//
//        JavaPairRDD<String,String> test = links.flatMapToPair(s -> {
//            List<Tuple2<String, String>> results = new ArrayList<>();
//            for (String n : s._2.toString().substring(1,s._2.toString().length()-1).split(" ")) {
//                results.add(new Tuple2<>(s._1(), n));
//            }
//            return results.iterator();
//        });
//        //        System.out.println("Test: "+test.collect());
//        /*
//        1: 2 3 4
//        2: 1 4
//        3: 1
//        4: 2 3
//
//        Link Content: [(4,[2 3]), (2,[1 4]), (3,[1]), (1,[2 3 4])]
//         */
//
//        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);
//        System.out.println("Link: " +links.collect()+ " Ranks: "+ranks.collect()+" Join values: "+links.join(ranks).values().collect());
//        for (int current = 0; current < ITERATIONS; current++) {
//            // Calculates URL contributions to the rank of other URLs.
//            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
//                    .flatMapToPair(s -> {
////                        int urlCount = Iterables.size(s._1());
//                        int urlCount = s._1().toString().split(" ").length;
//                        List<Tuple2<String, Double>> results = new ArrayList<>();
//                        for (String n : s._1.toString().substring(1,s._1.toString().length()-1).split(" ")) {
////                        for (String n : s._1) {
//
//                            results.add(new Tuple2<>(n, s._2() / urlCount));
//                        }
//                        return results.iterator();
//                    });
////            ranks = contribs.reduceByKey(new Sum());
//            System.out.println("Current: "+current+" Ranks:"+ ranks.collect());
//            ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
//
//
//        }
//        /*
//        Joins: [(14,([4116750],1.0)), (20,([2402613],1.0)), (19,([2402613],1.0)), (15,([4095634],1.0)), (18,([4207272],1.0)), (16,([5534647],1.0)), (22,([4095634],1.0)),
//         (17,([5703728],1.0)), (13,([5534647],1.0)), (24,([205444 530901 1601519 2583882 3072654 3492254 3498305 4096317 4189168 4638601 4751151 5242252],1.0)), (23,([5688890],1.0))]
//
//        */
//
////        List<Tuple2<String, Double>> output = ranks.collect();
////        for (Tuple2<?,?> tuple : output) {
////            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
////        }
//        JavaRDD<PageRank> out = ranks.map(line -> {
//            PageRank val = new PageRank();
//            val.setId(line._1());
//            val.setRank(line._2());
//            return val;
//        });
//        Dataset<Row> sql = spark.createDataFrame(out, PageRank.class);
//        sql.createOrReplaceTempView("ranksql");
//        sql.show();
//        Dataset<Row> top10 = spark.sql("SELECT id FROM ranksql ORDER BY rank DESC LIMIT 10");
//        top10.show();
//
//        spark.stop();
//
//
//    }
}
