package batch;

import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.List;

public class Top10 {
    public static void top10(JavaSparkContext sc) {

        List<Tuple2<String, Integer>> top10 = sc.textFile("hdfs://namenode:9000/input/title.principals.tsv")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> l[3].contains("actor") || l[3].contains("actress"))
                .mapToPair(l -> new Tuple2<>(l[2], 1))
                .foldByKey(0, Integer::sum)
                .mapToPair(t -> new Tuple2<>(t._2,t._1))
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<>(t._2,t._1))
                .take(10);

        System.out.println("\n\n TOP10: " + top10.toString() +  "\n\n");
    }
}
