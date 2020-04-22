package ggcd;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LoadMovies {
    // Movie Info to HBase
    public static class MovieMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (!fields[0].equals("tconst")){
                Put put = new Put(Bytes.toBytes(fields[0]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("TitleType"), Bytes.toBytes(fields[1]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("PrimaryTitle"), Bytes.toBytes(fields[2]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("OriginalTitle"), Bytes.toBytes(fields[3]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("isAdult"), Bytes.toBytes(fields[4]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("StartYear"), Bytes.toBytes(fields[5]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("EndYear"), Bytes.toBytes(fields[6]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Runtime"), Bytes.toBytes(fields[7]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Genres"), Bytes.toBytes(fields[8]));
                context.write(null, put);
            }
        }
    }

    // Movie Rating to HBase
    public static class RatingMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (!fields[0].equals("tconst")) {
                Put put = new Put(Bytes.toBytes(fields[0]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Rating"), Bytes.toBytes(fields[1]));
                context.write(null, put);
            }
        }
    }
}
