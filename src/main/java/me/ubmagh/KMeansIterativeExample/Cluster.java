package me.ubmagh.KMeansIterativeExample;

import java.util.ArrayList;
import java.util.List;

public class Cluster {

    private double center;
    private List<Double> points;

    public Cluster(double center) {
        this.center = center;
        points = new ArrayList<>();
    }


    public double getCenter() {
        return center;
    }

    public void setCenter(double center) {
        this.center = center;
    }

    public List<Double> getPoints() {
        return points;
    }

    public void setPoints(List<Double> points) {
        this.points = points;
    }
}
