package me.ubmagh.KMeansIterativeExample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Application {
    private static final int NBRPOINTS= 100;
    private static final int NBRCLUSTERS= 3;

    public static void main(String[] args) {
        List<Double> points = new ArrayList<Double>();
        List<Cluster> clusters = new ArrayList<Cluster>();
        for( int i=0; i<NBRPOINTS; i++){
            points.add( (Math.random()*1113737)%NBRPOINTS );
        }

        for( int i=0; i<NBRCLUSTERS; i++){
            clusters.add( new Cluster((Math.random()*137)%100) );
        }

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
            b = oldCenter1!=clusters.get(0).getCenter() && oldCenter2!=clusters.get(1).getCenter() && oldCenter3!=clusters.get(2).getCenter();

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
