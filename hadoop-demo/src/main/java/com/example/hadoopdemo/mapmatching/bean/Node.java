package com.example.hadoopdemo.mapmatching.bean;

import com.example.hadoopdemo.mapmatching.RTree.Rectangle;

/**
 * Created by AJQK on 2019/12/27.
 */
public class Node implements Geometry {
    private int nodeid;
    private double x;
    private double y;

    public int getNodeid() {
        return nodeid;
    }

    public void setNodeid(int nodeid) {
        this.nodeid = nodeid;
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
    public Rectangle getExtent() {
        return new Rectangle(x,x,y,y);
    }

    @Override
    public boolean intersect(Rectangle rectangle) {
        return x<=rectangle.getXmax() && x>=rectangle.getXmin() && y<=rectangle.getYmax() && y>=rectangle.getYmin();
    }
}
