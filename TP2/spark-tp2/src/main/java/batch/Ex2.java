package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import static batch.Friends.friends;
import static batch.Ratings.ratings;
import static batch.Top10.top10;

public class Ex2 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Batch");
        JavaSparkContext sc = new JavaSparkContext(conf);
        long time = System.currentTimeMillis();

        top10(sc);
        ratings(sc);
        //friends(sc);
        System.out.println("\n\nTime: " + ((System.currentTimeMillis() - time)) + "ms\n\n");
    }
}
