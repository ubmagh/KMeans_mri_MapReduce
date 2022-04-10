package me.ubmagh.KMeansMapReduce_1dpoints;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KmeansMapper extends Mapper<LongWritable, Text, DoubleWritable,DoubleWritable> {
    List<Double> centers=new ArrayList<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
        centers.clear();
        URI uri[]= context.getCacheFiles();
        FileSystem fs=FileSystem.get(context.getConfiguration());
        InputStreamReader is=new InputStreamReader(fs.open(new Path(uri[0])));
        BufferedReader br=new BufferedReader(is);
        String ligne=null;
        while ((ligne=br.readLine())!=null){
            String []values = ligne.split(" ");
            centers.add(Double.parseDouble(values[0]));
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
      double p=Double.parseDouble(value.toString());
      double min=Double.MAX_VALUE,d,nearest_center=0;
        for (double c:centers) {
            d=Math.abs(p-c);
            if (d<min){
                min=d;
                nearest_center=c;
            }
        }
        context.write(new DoubleWritable(nearest_center),new DoubleWritable(p));
    }

}
