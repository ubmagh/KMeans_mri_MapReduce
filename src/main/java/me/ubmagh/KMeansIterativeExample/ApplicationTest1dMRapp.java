package me.ubmagh.KMeansIterativeExample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ApplicationTest1dMRapp {
    private static final int NBRPOINTS= 26;
    private static final int NBRCLUSTERS= 3;

    public static void main(String[] args) {
        List<Double> points = new ArrayList<Double>();
        List<Cluster> clusters = new ArrayList<Cluster>();

        Stream.of( 10.d, 1.d, 2., 91d, 12d, 21d, 32.d, 78.d, 91.d, 0.d,
                99.d, 71.d, 37.d, 98.d, 16d, 37.d, 1.d, 11.d, 2.d,
                3.d, 4.d, 56.d, 66.d, 9.d, 8.d, 9.2d).forEach(number -> {
            points.add((Double) number);

        });
        clusters.add( new Cluster(10.3d) );
        clusters.add( new Cluster(4.5d) );
        clusters.add( new Cluster(9.2d) );


        // show centers & points (data)
        System.out.println("<===============[Data-points]===============>");
        for( int i=0; i<NBRPOINTS; i++){
            System.out.println( " points["+i+"]="+points.get(i) );
        }
        System.out.println("\n<===============[Centers]===============>");
        for( int i=0; i<NBRCLUSTERS; i++){
            System.out.println( " centers["+i+"]="+clusters.get(i).getCenter() );
        }



        double oldCenter1, oldCenter2, oldCenter3;
        double d1, d2, d3, min;
        boolean b;
        do {
            oldCenter1=clusters.get(0).getCenter();
            oldCenter2=clusters.get(1).getCenter();
            oldCenter3=clusters.get(2).getCenter();
            clusters.get(0).setPoints(new ArrayList<>());
            clusters.get(1).setPoints(new ArrayList<>());
            clusters.get(2).setPoints(new ArrayList<>());
            for (double p : points) {
                d1 = Math.abs(clusters.get(0).getCenter() - p);
                d2 = Math.abs(clusters.get(1).getCenter() - p);
                d3 = Math.abs(clusters.get(2).getCenter() - p);
                min = Math.min(Math.min(d1, d2), d3);
                if (min == d1) {
                    clusters.get(0).getPoints().add(p);
                } else {
                    if (min == d2)
                        clusters.get(1).getPoints().add(p);
                    else
                        clusters.get(2).getPoints().add(p);
                }
            }
            for (Cluster c : clusters) {
                double sum = 0;
                int count = 0;
                for (Double p : c.getPoints()) {
                    count++;
                    sum += p;
                }
                if( count>0 )
                c.setCenter(sum / count);
            }
            printClusters(clusters);
            b = oldCenter1!=clusters.get(0).getCenter() || oldCenter2!=clusters.get(1).getCenter() || oldCenter3!=clusters.get(2).getCenter();

        }while( b );


        System.out.println("\n\n###################################################> Finally <################################################### ");
        printClusters( clusters );
    }


    public static void printClusters( List<Cluster> clusters){
        System.out.println("\n==========================> Clusters");
        for( Cluster cluster:clusters)
            System.out.println(" Cluster Center: "+cluster.getCenter()+ " | Cluster points : "+ Arrays.toString(cluster.getPoints().toArray(new Double[0])));
    }

}
