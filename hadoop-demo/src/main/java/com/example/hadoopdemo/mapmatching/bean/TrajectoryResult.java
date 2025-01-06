package com.example.hadoopdemo.mapmatching.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrajectoryResult {
    private List<Point> points;
    private List<LineResult> lines;
    private int errorType;

    public TrajectoryResult() {
        points = new ArrayList<>();
        lines = new ArrayList<>();
    }


    public List<Point> getPoints() {
        return points;
    }

    public void setPoints(List<Point> points) {
        this.points = points;
    }

    public void addAll(TrajectoryResult result){
        points.addAll(result.getPoints());
        // 加上判断
        for(LineResult line:result.getLines()){
            addLine(line);
        }
    }

    public List<LineResult> getLines() {
        return lines;
    }

    public void setLines(List<LineResult> lines) {
        this.lines = lines;
    }
    // 20210125 不同线之间可能导致重复
    public void addLine(LineResult line){
        LineResult last = this.lines.size()==0?null:this.lines.get(this.lines.size()-1);
        if(last==null || last.getLineID() != line.getLineID()){
            this.lines.add(line);
        }else{
            last.setEndSpeed(line.getEndSpeed());
            last.setEndTime(line.getEndTime());
        }
    }

    public int getErrorType() {
        return errorType;
    }

    public void setErrorType(int errorType) {
        this.errorType = errorType;
    }
    public void updateErrorType(int errorType){
        this.errorType |= errorType;
    }
}
