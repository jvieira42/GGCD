package ggcd;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Top3 {
    // Actor-movies file Mapper
    public static class MovieActorMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if(!fields[1].equals("\\N")){
                String[] movies = fields[1].split(",");
                for(int i = 0; i < movies.length; i++)
                    context.write(new Text(movies[i]),new Text("A " + fields[0]));
            }
        }
    }

    // Movie-rating file Mapper
    public static class MovieRateMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            context.write(new Text(fields[0]),new Text("R " + fields[1]));
        }
    }

    // Actor-Movie#Rate Reducer to file
    public static class ActorMovieRateReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String person = "", rate = "";

            for(Text value : values){
                if(value.charAt(0) == 'R')
                    rate = value.toString().replace("R ","");
                else if(value.charAt(0) == 'A')
                    person = value.toString().replace("A ","");
            }
            if(!person.equals("") && !rate.equals("")){
                context.write(new Text(person), new Text(key + "#" + rate));
            }
        }
    }

    // Group values by key Mapper
    public static class Top3GroupMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t+");
            context.write(new Text(fields[0]), new Text(fields[1]));
        }
    }

    // Group values by key Reducer
    public static class Top3GroupReducer extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder s = new StringBuilder();
            for(Text value: values) {
                s.append(value);
                s.append(",");
            }
            context.write(key, new Text(s.toString()));
        }
    }

    // Compute top3 to HBase
    public static class Top3Mapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, Float> aux = new HashMap<>();
            Map<String, Float> top3 = new LinkedHashMap<>();

            String[] fields = value.toString().split("\t");
            String[] movies = fields[1].split(",");
            for(int i = 0; i < movies.length; i++){
                if(movies[i] != null){

                    String[] pair = movies[i].split("#", 2);
                    Float rate = Float.parseFloat(pair[1]);
                    aux.put(pair[0],rate);
                    top3 = aux.entrySet().stream()
                            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                            .limit(3)
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
                }
            }
            String top3String = top3.toString();
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("top3"), Bytes.toBytes(top3String));

            context.write(null, put);
        }
    }
}
