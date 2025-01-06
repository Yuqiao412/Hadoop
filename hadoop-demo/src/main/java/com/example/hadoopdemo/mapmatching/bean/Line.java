package com.example.hadoopdemo.mapmatching.bean;

import com.example.hadoopdemo.mapmatching.RTree.Rectangle;
import com.example.hadoopdemo.mapmatching.RTree.TreeHelper;

/**
 * Created by AJQK on 2019/12/27.
 */
public class Line implements Geometry {
    private int lineid;
    private int roadid;
    private int direction;
    private String list;
    private Point[] points = new Point[2];

    public int getLineid() {
        return lineid;
    }

    public void setLineid(int lineid) {
        this.lineid = lineid;
    }

    public int getRoadid() {
        return roadid;
    }

    public void setRoadid(int roadid) {
        this.roadid = roadid;
    }

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public String getList() {
        return list;
    }

    public void setList(String list) {
        this.list = list;
        String[] pointList=list.split("[,;]");
        points[0] = new Point(Double.parseDouble(pointList[0]), Double.parseDouble(pointList[1]));
        points[1] = new Point(Double.parseDouble(pointList[2]), Double.parseDouble(pointList[3]));
    }
    public Point getStart(){
        return points[0];
    }
    public Point getEnd(){
        return points[1];
    }

    @Override
    public Rectangle getExtent() {
        double xmin = Math.min(points[0].getX(), points[1].getX());
        double xmax = Math.max(points[0].getX(), points[1].getX());
        double ymin = Math.min(points[0].getY(), points[1].getY());
        double ymax = Math.max(points[0].getY(), points[1].getY());
        return new Rectangle(xmin,xmax,ymin,ymax);
    }

    @Override
    public boolean intersect(Rectangle rectangle) {
        //如果有点在面内肯定是相交了
        if(points[0].getX()<=rectangle.getXmax() && points[0].getX()>=rectangle.getXmin() &&
                points[0].getY()<=rectangle.getYmax() && points[0].getY()>=rectangle.getYmin())
            return true;
        if(points[1].getX()<=rectangle.getXmax() && points[1].getX()>=rectangle.getXmin() &&
                points[1].getY()<=rectangle.getYmax() && points[1].getY()>=rectangle.getYmin())
            return true;
        if(TreeHelper.intersect(points[0].getX(),points[0].getY(),points[1].getX(),points[1].getY(),
                rectangle.getXmin(),rectangle.getYmin(),rectangle.getXmin(),rectangle.getYmax()))
            return true;
        if(TreeHelper.intersect(points[0].getX(),points[0].getY(),points[1].getX(),points[1].getY(),
                rectangle.getXmin(),rectangle.getYmin(),rectangle.getXmax(),rectangle.getYmin()))
            return true;
        if(TreeHelper.intersect(points[0].getX(),points[0].getY(),points[1].getX(),points[1].getY(),
                rectangle.getXmin(),rectangle.getYmax(),rectangle.getXmax(),rectangle.getYmax()))
            return true;
        if(TreeHelper.intersect(points[0].getX(),points[0].getY(),points[1].getX(),points[1].getY(),
                rectangle.getXmax(),rectangle.getYmin(),rectangle.getXmax(),rectangle.getYmax()))
            return true;
        return false;
    }

}
