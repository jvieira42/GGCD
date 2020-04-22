package ggcd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        Connection conn = ConnectionFactory.createConnection(conf);

        // Criar tabelas movies e actors
        Admin admin = conn.getAdmin();

        HTableDescriptor m = new HTableDescriptor(TableName.valueOf("movies"));
        HTableDescriptor a = new HTableDescriptor(TableName.valueOf("actors"));
        m.addFamily(new HColumnDescriptor("Details"));
        a.addFamily(new HColumnDescriptor("Details"));

        admin.createTable(m);
        admin.createTable(a);

        admin.close();

        //-----------------------------LOAD MOVIE DATA-------------------------------------
        Job job0 = Job.getInstance(conf,"load-movies");
        job0.setJarByClass(Main.class);
        job0.setNumReduceTasks(0);
        job0.setOutputKeyClass(NullWritable.class);
        job0.setOutputValueClass(Put.class);

        job0.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job0, new Path("hdfs://namenode:9000/input/title.basics.tsv.bz2"), TextInputFormat.class, LoadMovies.MovieMapper.class);
        MultipleInputs.addInputPath(job0, new Path("hdfs://namenode:9000/input/title.ratings.tsv.bz2"), TextInputFormat.class, LoadMovies.RatingMapper.class);

        job0.setOutputFormatClass(TableOutputFormat.class);
        job0.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "movies");

        job0.waitForCompletion(true);

        //-----------------------------ACTOR INFO-------------------------------------
        Job job1 = Job.getInstance(conf, "load-info");

        job1.setJarByClass(Main.class);
        job1.setMapperClass(ActorInfo.ActorInfoMapper.class);

        job1.setNumReduceTasks(0);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Put.class);

        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job1, "hdfs://namenode:9000/input/name.basics.tsv.bz2");

        job1.setOutputFormatClass(TableOutputFormat.class);
        job1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors");

        job1.waitForCompletion(true);



        //-------------------------TOTAL MOVIES BY ACTOR-----------------------------------
        // Create Actor Movies File
        Job job2 = Job.getInstance(conf, "actor-movies");

        job2.setJarByClass(Main.class);
        job2.setMapperClass(ActorInfo.ActorMoviesMapper.class);
        job2.setCombinerClass(ActorInfo.ActorMoviesReducer.class);
        job2.setReducerClass(ActorInfo.ActorMoviesReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job2, "hdfs://namenode:9000/input/title.principals.tsv.bz2");

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("hdfs://namenode:9000/output/actorMovies"));

        job2.waitForCompletion(true);

        // Insert totalMovies to HBase
        Job job3 = Job.getInstance(conf, "total-actor-movies");

        job3.setJarByClass(Main.class);
        job3.setMapperClass(ActorInfo.TotalMoviesMapper.class);

        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Put.class);

        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job3, "hdfs://namenode:9000/output/actorMovies/part-r-00000");

        job3.setOutputFormatClass(TableOutputFormat.class);
        job3.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors");

        job3.waitForCompletion(true);



        //-------------------------TOP3 MOVIES BY ACTOR-----------------------------------
        // Create MovieRate by Actor File
        Job job4 = Job.getInstance(conf,"actor-movie-rate");
        job4.setJarByClass(Main.class);
        job4.setReducerClass(Top3.ActorMovieRateReducer.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);

        MultipleInputs.addInputPath(job4, new Path("hdfs://namenode:9000/output/actorMovies/part-r-00000"), TextInputFormat.class, Top3.MovieActorMapper.class);
        MultipleInputs.addInputPath(job4, new Path("hdfs://namenode:9000/input/title.ratings.tsv.bz2"), TextInputFormat.class, Top3.MovieRateMapper.class);

        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4, new Path("hdfs://namenode:9000/output/top3List"));

        job4.waitForCompletion(true);

        //Group MovieRate entries by Actor
        Job job5 = Job.getInstance(conf, "top3-group");

        job5.setJarByClass(Main.class);
        job5.setMapperClass(Top3.Top3GroupMapper.class);
        job5.setCombinerClass(Top3.Top3GroupReducer.class);
        job5.setReducerClass(Top3.Top3GroupReducer.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job5, "hdfs://namenode:9000/output/top3List/part-r-00000");

        job5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job5, new Path("hdfs://namenode:9000/output/top3Group/"));

        job5.waitForCompletion(true);

        // Compute Top3 and Insert to HBase
        Job job6 = Job.getInstance(conf,"top3");
        job6.setJarByClass(Main.class);
        job6.setMapperClass(Top3.Top3Mapper.class);
        job6.setNumReduceTasks(0);
        job6.setOutputKeyClass(NullWritable.class);
        job6.setOutputValueClass(Put.class);

        job6.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job6, "hdfs://namenode:9000/output/top3Group/part-r-00000");

        job6.setOutputFormatClass(TableOutputFormat.class);

        job6.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors");

        job6.waitForCompletion(true);


        //-------------------------SET OF COLLABORATORS BY ACTOR-----------------------------------

        // Create Movie Actors File
        Job job7 = Job.getInstance(conf,"movie-actors");
        job7.setJarByClass(Main.class);
        job7.setMapperClass(Collaborators.MovieActorsMapper.class);
        job7.setCombinerClass(Collaborators.MovieActorsReducer.class);
        job7.setReducerClass(Collaborators.MovieActorsReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        job7.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job7, "hdfs://namenode:9000/input/title.principals.tsv.bz2");

        job7.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job7, new Path("hdfs://namenode:9000/output/movieActors"));

        job7.waitForCompletion(true);


        // Create Actor-Collaborator entries File
        Job job8 = Job.getInstance(conf,"actor-collab");

        job8.setJarByClass(Main.class);
        job8.setReducerClass(Collaborators.ActorCollabReducer.class);

        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(Text.class);
        job8.setInputFormatClass(TextInputFormat.class);

        MultipleInputs.addInputPath(job8, new Path("hdfs://namenode:9000/output/movieActors/part-r-00000"), TextInputFormat.class, Collaborators.MovieCollabMapper.class);
        MultipleInputs.addInputPath(job8, new Path("hdfs://namenode:9000/input/title.principals.tsv.bz2"), TextInputFormat.class, Collaborators.MovieActorEntryMapper.class);

        job8.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job8, new Path("hdfs://namenode:9000/output/actorCollab"));

        job8.waitForCompletion(true);

        // Group Collaborators by Actor File
        Job job9 = Job.getInstance(conf, "actor-collab-group");

        job9.setJarByClass(Main.class);
        job9.setMapperClass(Collaborators.CollabGroupMapper.class);
        job9.setCombinerClass(Collaborators.CollabGroupReducer.class);
        job9.setReducerClass(Collaborators.CollabGroupReducer.class);

        job9.setOutputKeyClass(Text.class);
        job9.setOutputValueClass(Text.class);

        job9.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job9, "hdfs://namenode:9000/output/actorCollab/part-r-00000");

        job9.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job9, new Path("hdfs://namenode:9000/output/actorCollabGroup"));

        job9.waitForCompletion(true);


        // Compute Collabs and Insert to HBase
        Job job10 = Job.getInstance(conf,"top3");
        job10.setJarByClass(Main.class);
        job10.setMapperClass(Collaborators.CollabMapper.class);
        job10.setNumReduceTasks(0);
        job10.setOutputKeyClass(NullWritable.class);
        job10.setOutputValueClass(Put.class);

        job10.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job10, "hdfs://namenode:9000/output/actorCollabGroup/part-r-00000");

        job10.setOutputFormatClass(TableOutputFormat.class);

        job10.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors");

        job10.waitForCompletion(true);

        conn.close();

        System.out.println("\n\nTime Populating: " + ((System.currentTimeMillis() - time)/1000));
    }
}

