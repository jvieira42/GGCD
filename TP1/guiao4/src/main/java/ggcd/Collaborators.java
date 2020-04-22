package ggcd;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Collaborators {
    //-------------------------SET OF COLLABORATORS BY ACTOR-----------------------------------
    // Actors by Movie File
    public static class MovieActorsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (!fields[0].equals("tconst")){
                context.write(new Text(fields[0]),new Text(fields[2]));
            }
        }
    }

    public static class MovieActorsReducer extends Reducer<Text, Text, Text, Text> {
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

    // Create Actor-Collaborator entries File
    public static class MovieCollabMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            context.write(new Text(fields[0]),new Text("C " + fields[1]));
        }
    }

    public static class MovieActorEntryMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if(!fields[0].equals("tconst"))
                context.write(new Text(fields[0]),new Text("A " + fields[2]));
        }
    }

    public static class ActorCollabReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String actor = "", collabList = "";

            for(Text value : values){
                if(value.charAt(0) == 'A')
                    actor = value.toString().replace("A ","");
                else if(value.charAt(0) == 'C')
                    collabList = value.toString().replace("C ","");
            }
            context.write(new Text(actor), new Text(collabList));
        }
    }

    public static class CollabGroupMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            context.write(new Text(fields[0]), new Text(fields[1]));
        }
    }

    public static class CollabGroupReducer extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder s = new StringBuilder();
            for(Text value: values) {
                s.append(value);
            }
            context.write(key, new Text(s.toString()));
        }
    }

    public static class CollabMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Set<String> collabSet = new HashSet<>();

            String[] fields = value.toString().split("\t");
            String[] actors = fields[1].split(",");
            for(int i = 0; i < actors.length; i++){
                if(!actors[i].equals(fields[0]))
                    collabSet.add(actors[i]);
            }

            String collaborators = collabSet.toString();
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("collaborators"), Bytes.toBytes(collaborators));

            context.write(null, put);
        }
    }
}
