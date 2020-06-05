package batch;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.Iterator;
import  org.apache.hadoop.fs.Path;
import scala.Tuple2;

public class Ratings {
    public static void ratings(JavaSparkContext sc) throws Exception {
        String hdfsPrefix = "hdfs://namenode:9000";

        FileSystem fs = FileSystem.get(new URI(hdfsPrefix), sc.hadoopConfiguration());
        RemoteIterator<LocatedFileStatus> files = fs.listFiles( new Path("/logs/"),true);

        while (files.hasNext()) {
            String path = files.next().getPath().toUri().getPath();

            if(path.contains("/part-00000")){
                JavaPairRDD<String, Tuple2<Double,Integer>> mainFile = sc.textFile(hdfsPrefix + "/input/title.ratings.tsv")
                        .map(l -> l.split("\t"))
                        .filter(l -> !l[0].equals("tconst"))
                        .mapToPair(l -> new Tuple2<>(l[0],new Tuple2<>(Double.parseDouble(l[1]),Integer.parseInt(l[2]))))
                        .cache();


                sc.textFile(hdfsPrefix + path)
                        .map(l -> l.replaceAll("[()]",""))
                        .map(l -> l.split(","))
                        .mapToPair(l -> new Tuple2<>(l[0],Double.parseDouble(l[1])))
                        .groupByKey()
                        .fullOuterJoin(mainFile)
                        .mapToPair(l -> {
                            if(l._2._1.isPresent()){
                                Integer size = l._2._2.get()._2;
                                Double avg = l._2._2.get()._1;

                                Iterator<Double> it = l._2._1.get().iterator();
                                while (it.hasNext()) {
                                    Double val = it.next();
                                    avg = Double.parseDouble(new DecimalFormat("#.#").format(addToAverage(avg,size,val)));
                                    size++;

                                }
                                return new Tuple2<>(l._1,new Tuple2<>(avg,size));
                            }else{
                                return new Tuple2<>(l._1,l._2._2.get());
                            }
                        })
                        .map(l -> l._1 + "\t" + l._2._1 + "\t" + l._2._2)
                        .coalesce(1,true)
                        .saveAsTextFile(hdfsPrefix + "/tmp/");


                fs.delete(new Path("/input/title.ratings.tsv"),false);
                fs.rename(new Path("/tmp/part-00000"),new Path("/input/title.ratings.tsv"));
                fs.delete(new Path("/tmp"),true);
            }
        }
    }

    private static double addToAverage(double average, int size, double value) {
        return (size * average + value) / (size + 1);
    }
}
