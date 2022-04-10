package me.ubmagh.KMeansMapReduce_image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;



public class KmeansReducer extends Reducer<PixelWritable, PixelWritable, Text, Text> {

    public boolean finished = true;
    private List<PixelWritable> centers=new ArrayList<>();

    private List<List<PixelWritable>> clusters = new ArrayList<>();

    @Override
    protected void reduce(PixelWritable key, Iterable<PixelWritable> values, Reducer<PixelWritable, PixelWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        int somme=0;
        int nb_points=0;
        Iterator<PixelWritable> it=values.iterator();
        List<PixelWritable> cluster = new ArrayList<>();
        while (it.hasNext()){
            PixelWritable pixel = new PixelWritable(it.next());
            cluster.add( pixel );
            somme +=  pixel.getValue();
            nb_points++;
        }
        int mean= somme/nb_points;
        // return old_center AND new_center
        PixelWritable newCenter = new PixelWritable( -1, -1, mean);

        finished &= newCenter.equals(key);
        centers.add( newCenter );
        // in output file, i'll print newest_center then oldest_center
        context.write( new Text(  newCenter.getX()+" "+ newCenter.getY()), new Text(newCenter.getValue()+"") );
        clusters.add(cluster);
    }

    @Override
    protected void cleanup(Reducer<PixelWritable, PixelWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if( finished ) {
            KmeansDriver.finish( clusters );
        }else{
            Configuration conf = ConnectionToHDFS.getConf();
            try {
                FileSystem fs= FileSystem.get( URI.create(ConnectionToHDFS.getUrl()),conf);
                FSDataOutputStream fsdo = fs.create( new Path( KmeansDriver.path_to_centers) );
                BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( fsdo ) );
                for( int i=0; i<centers.size(); i++){
                    bw.write(""+centers.get(i));
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
