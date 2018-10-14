import com.google.common.collect.Iterators;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WikiBomb {
    private static final Pattern SPACES = Pattern.compile(":\\s+");
    public static class Sum implements Function2<Double, Double, Double> {
        public Double call(Double a, Double b) {
            return a + b;
        }
    }
    public static void main(String[] args) throws Exception {
        Integer ITERATIONS = 25;
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        // Create Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaPageRank")
                .config("spark.master", "local[1]")
                .getOrCreate();
        spark.conf().set("spark.driver.maxResultSize", "10g");
        // Create dataset from links file.
        JavaRDD<String> lines = spark.read().textFile(String.valueOf(args[0])).javaRDD();
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] parts = SPACES.split(s);
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey().cache();
        JavaRDD<HelperClass.LinkClass> linesOut = links.map(line -> {
            HelperClass.LinkClass val = new HelperClass.LinkClass();
//            if(title.col("id").contains(line._1()).equals("contains(id, "+line._1+")")) {
//            System.out.println("YES");
//            }
            val.setFrom(line._1());
            val.setTo(line._2());
            return val;
        });
        System.out.println(links.collect());
        Dataset<Row> linkTable = spark.createDataFrame(linesOut, HelperClass.LinkClass.class);
        linkTable.createOrReplaceTempView("linktbl");

        JavaPairRDD<Long,String> titles = spark.read().textFile(args[1]).javaRDD().zipWithIndex().mapToPair(x-> new Tuple2(x._2()+1,x._1()));
        JavaRDD<HelperClass.TitleClass> titleOut = titles.map(line -> {
            HelperClass.TitleClass val = new HelperClass.TitleClass();
            val.setId(line._1());
            val.setTitle(line._2());
            return val;
        });
        Dataset<Row> titleTable = spark.createDataFrame(titleOut, HelperClass.TitleClass.class);
        titleTable.createOrReplaceTempView("titletbl");
//        Dataset<Row> title =  spark.sql("Select id FROM titletbl WHERE title  LIKE '%surfing%' ");
//        Dataset title =  spark.sql("SELECT from as id FROM linktbl WHERE from IN (Select id FROM titletbl WHERE title  LIKE '%surfing%')");
        JavaRDD title2 =  spark.sql("SELECT from as id, to as link FROM linktbl WHERE from IN (Select id FROM titletbl WHERE title  LIKE '%surfing%')").rdd().toJavaRDD();
//         rocky =  spark.sql("Select id FROM titletbl WHERE title  LIKE 'Rocky_Mountain_National_Park'").collect();
        String rocky = spark.sql("Select id,id FROM titletbl WHERE title  LIKE 'Rocky_Mountain_National_Park'").rdd().toJavaRDD().collect().get(0).get(0).toString();
//        title.show();
//        JavaPairRDD rocky_id = title.rdd().toJavaRDD().mapToPair(x->{ return new Tuple2<>(x.toString().substring(1,x.toString().length()-1), rocky);});
        JavaPairRDD surfingFromTo = title2.rdd().toJavaRDD().mapToPair(x-> {String[] parts = x.toString().split(",");
            ArrayList<String> out = new ArrayList<>();
            for (String n: parts[1].toString().substring(13,parts[1].toString().length()-2).replace("[]","").split(" ")) {
                out.add(n);
            }
            out.add(rocky);
            Iterable<String> output = out;
//            Iterable<String> iterable = Arrays.asList(out);
            return new Tuple2(parts[0].toString().substring(1,parts[0].length()),output);
        } );
        System.out.println("Added rocky to surfing: "+surfingFromTo.collect());

        // Compute the page rank in given search.
        JavaPairRDD<String, Double> ranks = surfingFromTo.mapValues(rs -> 1.0);
//        System.out.println("Rank :"+ ranks.collect());
        JavaPairRDD<String, Iterable<String>> mylinks = surfingFromTo.mapToPair(s-> {
            System.out.println(s.toString());
            String[] parts = s.toString().split(",\\[");
            String part = parts[1].toString().replace("])","");
            return new Tuple2<>(parts[0].substring(1,parts[0].toString().length()), part);
        }).distinct().groupByKey().cache();
        System.out.println(mylinks.collect());
        for (int current = 0; current < 2; current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = mylinks.join(ranks).values()
                    .flatMapToPair(s -> {
//                        int urlCount = Iterables.size(s._1());
                        int urlCount = s._1().toString().split(",").length;
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1.toString().substring(1,s._1.toString().length()-1).split(",")) {
                            results.add(new Tuple2<>(n.replace(" ",""), s._2() / urlCount));
                        }
                        return results.iterator();
                    });
//            System.out.println("Contrib: "+contribs.collect());
//            System.out.println("Join: "+links.join(ranks).values().collect());
//            if (args.length > 2 && args[2].equals("tax")){
//                ranks = contribs.reduceByKey(new PageRank.Sum()).mapValues(sum -> 0.15 + sum * 0.85);
//            }
//            else ranks = contribs.reduceByKey(new PageRank.Sum());
            ranks = contribs.reduceByKey(new Sum());
            System.out.println(contribs.collect());
        }
        System.out.println("Ranks:"+ranks.collect());
        JavaRDD<HelperClass.PageRank> out = ranks.map(line -> {
            HelperClass.PageRank val = new HelperClass.PageRank();
            val.setId(line._1());
            val.setRank(line._2());
            return val;
        });
        Dataset<Row> rankTable = spark.createDataFrame(out, HelperClass.PageRank.class).cache();
        rankTable.createOrReplaceTempView("ranktbl");
        spark.sql("SELECT id,title FROM titletbl").show();
        spark.sql("SELECT id,rank FROM ranktbl ORDER BY rank DESC").show();
        Dataset output = spark.sql("SELECT titletbl.id, titletbl.title, ranktbl.rank from titletbl INNER JOIN ranktbl ON titletbl.id=ranktbl.id");
        output.createOrReplaceTempView("finaltbl");
        spark.sql("SELECT id,title,rank FROM finaltbl ORDER BY rank DESC").show();
//        Dataset<Row> top10 = spark.sql("SELECT id,title FROM titletbl WHERE id IN (SELECT id FROM ranktbl ORDER BY rank DESC LIMIT 10)").show();
//        top10.show();


        spark.stop();

    }
}