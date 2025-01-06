package com.example.hadoopdemo.mapmatching.HMM.bean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CandidateItem {
    private CandidatePoint point; // 借用IVMM的参与点 因为思路一致
    private double score;
    private CandidateItem last = null;

    //20210307 记录途径的路段 方便轨迹还原
    private List<DriveSegment> lines;
    private double distance;

    public CandidateItem() {
    }

    public CandidateItem(CandidatePoint point) {
        this.point = point;
    }

    public CandidatePoint getPoint() {
        return point;
    }

    public void setPoint(CandidatePoint point) {
        this.point = point;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public CandidateItem getLast() {
        return last;
    }

    public void setLast(CandidateItem last) {
        this.last = last;
    }

    public List<DriveSegment> getLines() {
        return lines;
    }

    public void setLines(List<DriveSegment> lines) {
        this.lines = lines;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }
}
