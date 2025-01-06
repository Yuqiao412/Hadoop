package com.example.hadoopdemo.mapmatching.mapreduce.baidu;

import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.utils.Transform;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TrajectoryPoint {
    private Point point;
    private Point mercatorPoint;
    private String day;
    private long time;
    private String content;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public TrajectoryPoint(String val) throws ParseException {
        this.content = val;
        String[] values = val.split("[,]");
        day = values[0];
        point = new Point(Double.parseDouble(values[4]),Double.parseDouble(values[5]));
        mercatorPoint = Transform.lonLat2Mercator(point);
        time = sdf.parse(values[0]+values[1]).getTime();
    }

    public Point getMercatorPoint() {
        return mercatorPoint;
    }

    public void setMercatorPoint(Point mercatorPoint) {
        this.mercatorPoint = mercatorPoint;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Point getPoint() {
        return point;
    }

    public void setPoint(Point point) {
        this.point = point;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}
