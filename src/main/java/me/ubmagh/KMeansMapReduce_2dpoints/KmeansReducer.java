package me.ubmagh.KMeansMapReduce_2dpoints;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KmeansReducer extends Reducer<PointWritable,PointWritable, Text, Text> {

    public boolean finished = true;
    private List<PointWritable> centers=new ArrayList<>();

    @Override
    protected void setup(Reducer<PointWritable, PointWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        finished = true;
    }

    @Override
    protected void reduce(PointWritable key, Iterable<PointWritable> values, Reducer<PointWritable,PointWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        double sommeX=0, sommeY=0;
        int nb_points=0;
        Iterator<PointWritable> it=values.iterator();
        while (it.hasNext()){
            PointWritable pw =  it.next();
            sommeX+= pw.getX();
            sommeY+= pw.getY();
            nb_points++;
        }
        double meanX=sommeX/nb_points;
        double meanY=sommeY/nb_points;
        // return old_center AND new_center
        PointWritable newCenter = new PointWritable( meanX, meanY);

        finished &= newCenter.equals(key);

        centers.add( newCenter);
        centers.add( key);
        // in output file, i'll print newest_center then oldest_center
        context.write( new Text(  newCenter.toString()), new Text(key.toString()) );
    }

    @Override
    protected void cleanup(Reducer<PointWritable, PointWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if( finished )
            KmeansDriver.setFinished(true);
        else{
            Configuration conf = ConnectionToHDFS.getConf();
            try {
                FileSystem fs= FileSystem.get( URI.create(ConnectionToHDFS.getUrl()),conf);
                FSDataOutputStream fsdo = fs.create( new Path("/input2/centers.txt") );
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
