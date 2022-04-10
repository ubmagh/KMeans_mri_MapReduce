package me.ubmagh.KMeansMapReduce_1dpoints;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

    target ~ $ hadoop jar kmeans-1.0-Any.jar me.ubmagh.KMeansMapReduce_1dpoints.KmeansDriver /input1/data.txt /output1/iteration
 */

public class KmeansDriver {

    private static boolean finished;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        Path input = new Path(args[0]);
        Path output = null;
        int i=0;
        while( !finished) {
            output = new Path(args[1]+(i++));
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Job Kmeans 1D points");
            job.setJarByClass(KmeansDriver.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //   job.addCacheFile(new URI("hdfs://localhost:9000/input/centers.txt"));
            job.addCacheFile(new URI("/input1/centers.txt"));

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
