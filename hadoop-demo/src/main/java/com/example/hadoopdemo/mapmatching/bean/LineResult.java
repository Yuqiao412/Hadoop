package com.example.hadoopdemo.mapmatching.bean;

/**
 *  先给个假定 路段的驶入时间 驶出时间 不一定是车辆经过路段起点及终点的时间
 */
public class LineResult {
    // 目前的驶入和驶出时间是按平均速度算的 有需要可以再添加加速度算的时间
    private long startTime;
    private long endTime;
    private long lineID;
    private long roadID;
    private double meanSpeed; //通行的平均速度 总路程除以总时间
    // 以下为推断的瞬时速度 线性插值或者直接取两侧轨迹点的速度（目前先用线性插值，加速度推断）
    private double startSpeed;
    private double endSpeed;

    public LineResult() {
    }

    public LineResult(long startTime, long endTime, long lineID, long roadID, double meanSpeed, double startSpeed, double endSpeed) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.lineID = lineID;
        this.roadID = roadID;
        this.meanSpeed = meanSpeed;
        this.startSpeed = startSpeed;
        this.endSpeed = endSpeed;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getLineID() {
        return lineID;
    }

    public void setLineID(long lineID) {
        this.lineID = lineID;
    }

    public long getRoadID() {
        return roadID;
    }

    public void setRoadID(long roadID) {
        this.roadID = roadID;
    }

    public double getStartSpeed() {
        return startSpeed;
    }

    public void setStartSpeed(double startSpeed) {
        this.startSpeed = startSpeed;
    }

    public double getEndSpeed() {
        return endSpeed;
    }

    public void setEndSpeed(double endSpeed) {
        this.endSpeed = endSpeed;
    }

    public double getMeanSpeed() {
        return meanSpeed;
    }

    public void setMeanSpeed(double meanSpeed) {
        this.meanSpeed = meanSpeed;
    }
}
