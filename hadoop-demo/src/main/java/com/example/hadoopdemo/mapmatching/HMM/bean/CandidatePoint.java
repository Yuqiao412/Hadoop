package com.example.hadoopdemo.mapmatching.HMM.bean;

import com.example.hadoopdemo.mapmatching.bean.Line;
import com.example.hadoopdemo.mapmatching.bean.Point;

import java.util.HashMap;
import java.util.Map;

public class CandidatePoint {
    private Point samplingPoint;
    private Point candidatePoint;
    private Line line;
    private double scale;//永远是距离start的比例
    private double height;
    private double r;// snail代码中给r加了一个惩罚函数

    public Point getSamplingPoint() {
        return samplingPoint;
    }

    public void setSamplingPoint(Point samplingPoint) {
        this.samplingPoint = samplingPoint;
    }

    public Point getCandidatePoint() {
        return candidatePoint;
    }

    public void setCandidatePoint(Point candidatePoint) {
        this.candidatePoint = candidatePoint;
    }

    public double getHeight() {
        return height;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    public Line getLine() {
        return line;
    }

    public void setLine(Line line) {
        this.line = line;
    }

    public double getScale() {
        return scale;
    }

    public void setScale(double scale) {
        this.scale = scale;
    }

    public double getR() {
        return r;
    }

    public void setR(double r) {
        this.r = r;
    }
}
