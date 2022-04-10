package me.ubmagh.KMeansMapReduce_2dpoints;

import org.apache.hadoop.io.WritableComparable;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PointWritable implements WritableComparable {

    private double x,y;

    public PointWritable() {
    }

    public PointWritable(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public PointWritable( PointWritable p) {
        this.x = p.getX();
        this.y = p.getY();
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    @Override
    public int compareTo(Object o) {
        PointWritable p = (PointWritable) o;
        if( p.getX()==this.getX() && p.getY()==this.getY() )
            return 0;
        if( p.getY()> this.getY() )
            return -1;
        if( p.getY()< this.getY() )
            return 1;

        if( this.getX()>p.getX())
            return 1;
        return -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(x);
        dataOutput.writeDouble(y);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        x = dataInput.readDouble();
        y = dataInput.readDouble();
    }

    public double calculateDistance( PointWritable pt){
        return Math.sqrt(  Math.pow( this.getX()-pt.getX(), 2) + Math.pow( this.getY() - pt.getY(), 2 )  );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PointWritable that = (PointWritable) o;
        return Double.compare(that.x, x) == 0 && Double.compare(that.y, y) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }

    @Override
    public String toString() {
        return "" + x +
                "," + y ;
    }
}
