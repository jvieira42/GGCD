package batch;

import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Friends {
    public static void friends(JavaSparkContext sc){

        List<Tuple2<String, Set<String>>> res = sc.textFile("hdfs://namenode:9000/input/title.principals.tsv")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> l[3].contains("actor") || l[3].contains("actress"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                .groupByKey()
                .mapToPair(l -> new Tuple2<>(l._1,
                        StreamSupport.stream(l._2.spliterator(),false)
                                .flatMap(s1 -> StreamSupport.stream(l._2.spliterator(),false)
                                        .map(s2 -> new Tuple2<>(s1,s2)))
                                .collect(Collectors.toSet())
                ))
                .map(l -> l._2)
                .flatMap(Set::iterator)
                .mapToPair(l -> l)
                .groupByKey()
                .mapToPair(l -> new Tuple2<>(l._1,
                        StreamSupport.stream(l._2.spliterator(),false)
                                .collect(Collectors.toSet())
                ))
                .filter(l -> l._2.remove(l._1))
                .collect();

        for(int i = 0; i < res.size(); i++) {
            System.out.println("\n\nEntry: " + res.get(i).toString());
        }
    }
}
