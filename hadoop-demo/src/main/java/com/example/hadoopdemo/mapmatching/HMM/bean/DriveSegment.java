package com.example.hadoopdemo.mapmatching.HMM.bean;

import com.example.hadoopdemo.mapmatching.bean.Line;

import java.util.List;

public class DriveSegment {
    private int roadid;
    private double length;
    private float speed;//单位为m/s

    // HMM实现轨迹还原 增加一个保存形状点的变量 20210307
    private List<Line> lines;
    private boolean direction;

    public DriveSegment() {
    }

    public DriveSegment(int roadid, double length, float speed) {
        this.roadid = roadid;
        this.length = length;
        this.speed = speed;
    }

    public DriveSegment(int roadid, double length, float speed, List<Line> lines, boolean direction) {
        this.roadid = roadid;
        this.length = length;
        this.speed = speed;
        this.lines = lines;
        this.direction = direction;
    }

    public List<Line> getLines() {
        return lines;
    }

    public void setLines(List<Line> lines) {
        this.lines = lines;
    }

    public boolean isDirection() {
        return direction;
    }

    public void setDirection(boolean direction) {
        this.direction = direction;
    }

    public int getRoadid() {
        return roadid;
    }

    public void setRoadid(int roadid) {
        this.roadid = roadid;
    }

    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }

    public float getSpeed() {
        return speed;
    }

    public void setSpeed(float speed) {
        this.speed = speed;
    }
}
