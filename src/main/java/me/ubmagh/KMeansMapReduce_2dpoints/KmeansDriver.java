package me.ubmagh.KMeansMapReduce_2dpoints;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/*
    after packing the app with maven & uploading txt files to hdfs ;
    run this job with the command :

    target ~ $ hadoop jar kmeans-1.0-Any.jar me.ubmagh.KMeansMapReduce_2dpoints.KmeansDriver /input2/data.txt /output2/iteration
 */
public class KmeansDriver {

    private static boolean finished=false;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        Path input = new Path(args[0]);
        Path output = null;
        int i = 0;
        while( !finished ) {
            Configuration conf = new Configuration();
            output = new Path( args[1]+(i++) );
            Job job = Job.getInstance(conf, "2D points Job Kmeans");
            job.setJarByClass(KmeansDriver.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setMapOutputKeyClass(PointWritable.class);
            job.setMapOutputValueClass(PointWritable.class);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            // job.addCacheFile(new URI("hdfs://localhost:9000/input/centers.txt"));
            job.addCacheFile(new URI("/input2/centers.txt"));
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            job.waitForCompletion(true);
        }
    }


    public static boolean isFinished() {
        return finished;
    }

    public static void setFinished(boolean finished) {
        KmeansDriver.finished = finished;
    }
}
