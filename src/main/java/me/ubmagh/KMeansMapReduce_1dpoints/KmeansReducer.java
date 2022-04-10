package me.ubmagh.KMeansMapReduce_1dpoints;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KmeansReducer extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable> {

    private List<Double> centers = new ArrayList<>();
    public boolean finished = true;


    @Override
    protected void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double somme=0;
        int nb_points=0;
        Iterator<DoubleWritable> it=values.iterator();
        while (it.hasNext()){
            somme+=it.next().get();
            nb_points++;
        }
        double mean=somme/nb_points;
        DoubleWritable newCenter = new DoubleWritable(mean);
        centers.add( newCenter.get());
        centers.add( key.get());
        finished &= newCenter.equals(key);
        context.write( key, newCenter);
    }

    @Override
    protected void cleanup(Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if( finished )
            KmeansDriver.setFinished(true);
        else{
            Configuration conf = me.ubmagh.KMeansMapReduce_2dpoints.ConnectionToHDFS.getConf();
            try {
                FileSystem fs= FileSystem.get( URI.create(ConnectionToHDFS.getUrl()),conf);
                FSDataOutputStream fsdo = fs.create( new Path("/input1/centers.txt") );
                BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( fsdo ) );
                for( int i=2; i<centers.size()+2; i+=2){
                    bw.write(""+centers.get(i-2)+" "+centers.get(i-1));
                    bw.newLine();
                }
                bw.flush();
                bw.close();
                fsdo.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
