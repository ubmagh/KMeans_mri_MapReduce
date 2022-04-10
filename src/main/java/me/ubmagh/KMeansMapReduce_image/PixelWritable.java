package me.ubmagh.KMeansMapReduce_image;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PixelWritable implements WritableComparable {

    private int x,y,value;


    public PixelWritable() {
    }

    public PixelWritable(int x, int y, int value) {
        this.x = x;
        this.y = y;
        this.value = value;
    }

    public PixelWritable(PixelWritable p) {
        this.x = p.getX();
        this.y = p.getY();
        this.value = p.getValue();
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public int compareTo(Object o) {
        PixelWritable p = (PixelWritable) o;
        if( p.getValue()==this.getValue()  )
            return 0;
        if( p.getValue()> this.getValue() )
            return -1;
        return 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(x);
        dataOutput.writeInt(y);
        dataOutput.writeInt(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        x = dataInput.readInt();
        y = dataInput.readInt();
        value = dataInput.readInt();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PixelWritable that = (PixelWritable) o;
        return Integer.compare(that.x, x) == 0 && Integer.compare(that.y, y) == 0 && Integer.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, value);
    }

    @Override
    public String toString() {
        return "" + x + " " + y+" "+value ;
    }

    public int calculateDistance( PixelWritable p){
        return Math.abs( p.getValue()-this.getValue() );
    }

}
