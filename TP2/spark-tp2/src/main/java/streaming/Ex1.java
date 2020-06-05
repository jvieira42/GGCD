package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.StreamSupport;

public class Ex1 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                //.setMaster("local[2]")
                .setAppName("Streaming");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(10));
        sc.checkpoint("hdfs://namenode:9000/stream");
        JavaPairDStream<String,Double> stream = sc.socketTextStream("streamgen", 12345)
                .map(t -> t.split("\t+"))
                .mapToPair(t -> new Tuple2<>(t[0],Double.parseDouble(t[1])));


        // PairRDD com id e nome dos filmes
        JavaPairRDD<String,String> movies = sc.sparkContext().textFile("hdfs://namenode:9000/input/title.basics.tsv")
                .map(t -> t.split("\t+"))
                .mapToPair(t -> new Tuple2<>(t[0],t[2]))
                .cache();

        //---------------------------------LOG---------------------------------

        stream.window(Durations.minutes(10),Durations.minutes(10))
                .foreachRDD(rdd -> {
                    LocalDateTime now = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH_mm");
                    String batchTime = now.format(formatter);

                    rdd.coalesce(1, true).saveAsTextFile("hdfs://namenode:9000/logs/" + batchTime);
                });


        //---------------------------------TOP3---------------------------------

        stream.window(Durations.minutes(10),Durations.seconds(60))
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(p._1,
                        StreamSupport.stream(p._2.spliterator(),false)
                                .mapToDouble(a -> a)
                                .average()
                                .getAsDouble()
                ))
                .foreachRDD(rdd -> {
                    List<Tuple2<Double, String>> res = rdd
                            .mapToPair(t -> new Tuple2<>(t._1,t._2))
                            .join(movies)
                            .mapToPair(t -> new Tuple2<>(t._2._1,t._2._2))
                            .sortByKey(false).take(3);

                    System.out.println("\n\nTOP3: " + res.toString() + "\n\n");
                });

        //---------------------------------TRENDING---------------------------------

        stream.window(Durations.minutes(15),Durations.minutes(15))
                .groupByKey()
                .mapToPair(p ->  new Tuple2<>(p._1,p._2.spliterator().getExactSizeIfKnown()))
                .mapWithState(StateSpec.function((String k, Optional<Long> v, State<Long> s) -> {
                    long noVotes;
                    if(v.isPresent() && s.exists()){
                        noVotes = v.get() > s.get() ? v.get() : 0;
                        s.update(noVotes);

                    }else if (v.isPresent() && !s.exists()){
                        noVotes = v.get();
                        s.update(noVotes);

                    }else {
                        noVotes = 0;
                        s.remove();
                    }

                    return new Tuple2<>(k,noVotes);
                }))
                .filter(l -> l._2!=0)
                .foreachRDD(rdd -> {
                    List<Tuple2<String, Long>> res = rdd.mapToPair(l -> new Tuple2<>(l._1,l._2))
                                .join(movies)
                                .mapToPair(l -> new Tuple2<>(l._2._2,l._2._1))
                                .collect();

                    System.out.println("\n\nTRENDING: " + res +"\n\n");
                });

        sc.start();
        sc.awaitTermination();
    }
}
