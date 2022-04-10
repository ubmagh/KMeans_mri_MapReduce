package me.ubmagh.KMeansMapReduce_image;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;


/*
    after packing the app with maven & uploading txt files to hdfs ;
    run this job with the command :

    target ~ $ hadoop jar kmeans-1.0-Any.jar me.ubmagh.KMeansMapReduce_image.KmeansDriver /input3/PHD.txt /output3
 */
public class KmeansDriver {

    private static boolean finished=false;
    private static Log log = LogFactory.getLog(KmeansDriver.class);
    private static String lastOutput;
    private static int image_height, image_width;
    private static final String imageExtension ="GIF"; //  "JPEG" "GIF"
    public static final String path_to_centers = "/input3/centers.txt";
    public static final String path_to_image = "/input3/brain_mri.gif"; //  "/input3/brain.jpeg"   "/input3/brain_mri.gif"

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        if( args.length!=2 ){
            log.fatal(" Need exactly 2 arguments  !!!");
            System.exit(-1);
        }

        Path input = new Path(args[0]);
        Path output ;
        lastOutput = args[1];
        int i=0;
        while( !finished ) {
            output= new Path( args[1]+"/iteration"+(i) );
            i++;
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "image Job Kmeans");
            job.setJarByClass(KmeansDriver.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setMapOutputKeyClass(PixelWritable.class);
            job.setMapOutputValueClass(PixelWritable.class);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass( TextInputFormat.class );
            job.setOutputFormatClass( TextOutputFormat.class);
            // job.addCacheFile(new URI("hdfs://localhost:9000/input/centers.txt"));
            job.addCacheFile(new URI(path_to_centers));
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            job.waitForCompletion(true);
        }
    }


    public static boolean isFinished() {
        return finished;
    }

    /*
    public static void setFinished(boolean finished) {
        KmeansDriver.finished = finished;
    }
    */


    public static void setImage_height(int image_height) {
        KmeansDriver.image_height = image_height;
    }

    public static void setImage_width(int image_width) {
        KmeansDriver.image_width = image_width;
    }

    public static Log getLog() {
        return log;
    }

    public static void finish( List<List<PixelWritable>> clusters ){
        KmeansDriver.finished = true; // stop looping on the job
        int i=1;
        log.fatal(" [KmeansDriver::finish]> Creating  ("+image_height+"x"+image_width+") images");
        for( List<PixelWritable> cluster:clusters){
            try {
                createImage( new Path(lastOutput+"/image_cluster_"+i+"."+imageExtension.toLowerCase()), cluster);
                createTxtFile( i, cluster);
                i++;
            } catch (IOException e) {
                log.fatal(" [KmeansDriver::finish]> Cannot create image ! (image"+i+")");
            }
        }
    }

    private static  void createTxtFile( int i, List<PixelWritable> cluster) throws  IOException{

        FileSystem  fs = FileSystem.get( ConnectionToHDFS.getConf());
        FSDataOutputStream ff = fs.create( new Path("/output3/pixels"+i+".txt"));
        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(ff));
        for( PixelWritable pixel : cluster){
            bw.write( pixel.toString() );
            bw.newLine();
        }
        bw.close();
        ff.close();

    }

    public static  void createTxtFile( int i, int[][] cluster) {
        try {
            FileSystem fs = FileSystem.get(ConnectionToHDFS.getConf());
            FSDataOutputStream ff = fs.create(new Path("/output3/pixels" + i + ".txt"));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(ff));
            for (int ii=0; ii<cluster.length; ii++) {
                for( int j=0; j<cluster[ii].length; j++){
                    bw.write(String.valueOf(cluster[ii][j])+"");
                    bw.newLine();
                }
                bw.flush();
            }
            bw.close();
            ff.close();
        }catch
        (Exception exc){
            log.warn(" Hi ");
        }
    }

    private static void createImage( Path path, List<PixelWritable> pixels  ) throws IOException {
        BufferedImage image = new BufferedImage( image_width, image_height, BufferedImage.TYPE_INT_RGB );
        FileSystem fs = FileSystem.get( ConnectionToHDFS.getConf() );
        FSDataOutputStream fos=fs.create(path);

        // write collected pixels
        pixels.forEach( pixel -> {
            int value = pixel.getValue();
            Color color = new Color( value, value, value);
            image.setRGB( pixel.getX(), pixel.getY(), color.getRGB() );
        } );

        ImageIO.write( image, imageExtension, fos );

        log.info(" image is saved : "+path.toString()+"   |  it is generated from a cluster with size : "+pixels.size()+" | Random pixel : "+pixels.get( ((int) Math.random()*1037)%pixels.size() ).toString());
    }
}
