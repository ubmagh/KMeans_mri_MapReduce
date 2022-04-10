package me.ubmagh.KMeansMapReduce_image;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KmeansMapper extends Mapper< LongWritable, Text, PixelWritable, PixelWritable> {

    List<PixelWritable> centers=new ArrayList<>();
    int[][] pixels_values;
    int image_height, image_width;
    private Log logger;

    @Override
    protected void setup(Mapper<LongWritable, Text, PixelWritable, PixelWritable>.Context context) throws IOException, InterruptedException {
        centers.clear();
        logger = KmeansDriver.getLog();
        URI uri[]= context.getCacheFiles();
        FileSystem fs=FileSystem.get(context.getConfiguration());
        InputStreamReader is=new InputStreamReader(fs.open(new Path(uri[0])));
        BufferedReader br=new BufferedReader(is);
        String ligne=null;
        while ((ligne=br.readLine())!=null){
            String [] values = ligne.split(" ");
            centers.add( new PixelWritable( Integer.parseInt( values[0]), Integer.parseInt( values[1]), Integer.parseInt( values[2]) ) );
        }

        logger.info(" [Mapper] centers filled , size : "+centers.size());

        Path path_to_image = new Path(KmeansDriver.path_to_image);
        BufferedImage image = ImageIO.read( fs.open( path_to_image ) );
        image_width = image.getWidth();
        image_height = image.getHeight();

        KmeansDriver.setImage_height( image_height );
        KmeansDriver.setImage_width( image_width );

        pixels_values = new int[image_width][image_height];

        // fill local matrix with image pixels
        for( int i=0; i<image_height; i++)
            for( int j=0; j<image_width; j++ ){
                int clr = image.getRGB( i,j);
                pixels_values[i][j] = clr & 0xFF;
            }
        // KmeansDriver.createTxtFile(0, pixels_values);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PixelWritable, PixelWritable>.Context context) throws IOException, InterruptedException {

        PixelWritable nearest_center=null;

        int d,min;
        PixelWritable newP;
        for ( int i=0; i<image_height; i++)
            for( int j=0; j<image_width; j++) {
                newP = new PixelWritable( i, j, pixels_values[i][j] );
                if( newP.getValue()==0 )continue; // ignore black background
                min = Integer.MAX_VALUE;
                nearest_center = null;
                for( PixelWritable c: centers){
                    d= c.calculateDistance(newP);
                    if (d<min){
                        min=d;
                        nearest_center= new PixelWritable(c);
                    }
                }
                context.write(nearest_center, newP);
                // logger.info(" [Mapper] nearest center : " + nearest_center.getValue());
            }
    }

}
