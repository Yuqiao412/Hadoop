package com.example.hadoopdemo.mapmatching.RTree;

/**
 * Created by AJQK on 2020/1/1.
 */
public class Centroid {
    private double x;
    private double y;

    public Centroid() {
    }

    public Centroid(double x, double y) {
        this.x = x;
        this.y = y;
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
}
