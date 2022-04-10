package me.ubmagh.KMeansMapReduce_2dpoints;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KmeansMapper extends Mapper<LongWritable, Text, PointWritable,PointWritable> {

    List<PointWritable> centers=new ArrayList<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, PointWritable, PointWritable>.Context context) throws IOException, InterruptedException {
        centers.clear();
        URI uri[]= context.getCacheFiles();
        FileSystem fs=FileSystem.get(context.getConfiguration());
        InputStreamReader is=new InputStreamReader(fs.open(new Path(uri[0])));
        BufferedReader br=new BufferedReader(is);
        String ligne=null;
        while ((ligne=br.readLine())!=null){
            String [] values = ligne.split(" ");
            String[] points = values[0].split(",");
            centers.add( new PointWritable( Double.parseDouble( points[0]), Double.parseDouble( points[1]) ) );
        }


    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PointWritable, PointWritable>.Context context) throws IOException, InterruptedException {
      String[] couple = value.toString().split(",");
      PointWritable p = new PointWritable( Double.parseDouble( couple[0]), Double.parseDouble( couple[1]) );

      double min=Double.MAX_VALUE,d;
      PointWritable nearest_center=new PointWritable(p);
        for (PointWritable c:centers) {
            d= c.calculateDistance(p);
            if (d<min){
                min=d;
                nearest_center=new PointWritable(c);
            }
        }
        context.write( nearest_center, p);
    }

}
