package com.example.hadoopdemo.mapmatching.utils;


import com.example.hadoopdemo.mapmatching.bean.Point;

public class Transform {
    public static Point lonLat2Mercator(Point lonLat)
    {
        double x = lonLat.getX() * 20037508.34 / 180;
        double y = Math.log(Math.tan((90 + lonLat.getY()) * Math.PI / 360)) / (Math.PI / 180);
        y = y * 20037508.34 / 180;
        return new Point(x,y);
    }
    //墨卡托转经纬度
    public static Point Mercator2lonLat(Point mercator)
    {
        double x = mercator.getX() / 20037508.34 * 180;
        double y = mercator.getY() / 20037508.34 * 180;
        y = 180 / Math.PI * (2 * Math.atan(Math.exp(y * Math.PI / 180)) - Math.PI / 2);
        return new Point(x,y);
    }
    public static Double getDistance(Point p1,Point p2){
    	return Math.sqrt(Math.pow(p1.getX()-p2.getX(), 2)+
				Math.pow(p1.getY()-p2.getY(), 2));
    }
    public static Double getEuclideanDistance(Point p1,Point p2){
    	Point p11 = lonLat2Mercator(p1);
    	Point p22 = lonLat2Mercator(p2);
    	return Math.sqrt(Math.pow(p11.getX()-p22.getX(), 2)+
				Math.pow(p11.getY()-p22.getY(), 2));
    }
    public static Double getEuclideanDistance2(Point p1, Point p2){
        double earthRadius = 6370996.81;
        double x1 = Math.toRadians(p1.getX());
        double x2 = Math.toRadians(p2.getX());
        double y1 = Math.toRadians(p1.getY());
        double y2 = Math.toRadians(p2.getY());
        double result = Math.sin(y1)*Math.sin(y2)+Math.cos(y1)*Math.cos(y2)*Math.cos(x2-x1);
        if(result>1.0)
            result = 1.0;
        return earthRadius*Math.acos(result);
    }
    public static Double getManhattanDistance(Point p1,Point p2){
    	Point p11 = lonLat2Mercator(p1);
    	Point p22 = lonLat2Mercator(p2);
    	return Math.abs(p11.getX()-p22.getX())+Math.abs(p11.getY()-p22.getY());
    }
    // 20201129 直接改为用最后的日期
    public static long Time2Second(long time){
        long tmp;
        long total = 0;
        long i = 1;
		do{
			tmp=time%100;
			total+=tmp*i;
			i*=60;
		}while((time=time/100)!=0);
		return total;
	}
    public static long Second2Time(long time){
        long i = 1;
        long total = 0;
        do{
            total += time % 60 * i;
            time/=60;
            i*=100;
        }while (time!=0);
        return total;
    }
}
