package ggcd;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ActorInfo {
    //-------------------------ACTOR INFO-----------------------------------
    // Actor Info to HBase
    public static class ActorInfoMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (!fields[0].equals("nconst")){
                Put put = new Put(Bytes.toBytes(fields[0]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("primaryName"), Bytes.toBytes(fields[1]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("birthYear"), Bytes.toBytes(fields[2]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("deathYear"), Bytes.toBytes(fields[3]));
                context.write(null, put);
            }
        }
    }

    //-------------------------No OF ACTOR MOVIES-----------------------------------
    // Movies by Actor Mapper
    public static class ActorMoviesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (!fields[0].equals("tconst")){
                context.write(new Text(fields[2]),new Text(fields[0]));
            }
        }
    }

    // Movies by Actor Reducer
    public static class ActorMoviesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            for (Text value : values){
                out.append(value);
                out.append(",");
            }
            context.write(key, new Text(out.toString()));
        }
    }

    // Compute Total Movies to HBase
    public static class TotalMoviesMapper extends Mapper<LongWritable, Text, NullWritable, Put>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            long total = fields[1].split(",").length;
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("totalMovies"), Bytes.toBytes(total));
            context.write(null, put);
        }
    }
}
