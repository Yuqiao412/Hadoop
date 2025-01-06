package com.example.hadoopdemo.mapmatching.utils;

import com.example.hadoopdemo.mapmatching.bean.LineResult;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.bean.TrajectoryResult;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ExportUtils {
    public static void exportPoint(TrajectoryResult result, String filename) throws IOException {
        FileWriter writer = new FileWriter(filename);

        for(Point p:result.getPoints()){
            String sb = p.getX() + "," +
                    p.getY() + "," +
                    p.getAngle() + "\n";
            writer.write(sb);
        }
        writer.flush();
        writer.close();
    }
    public static void exportLine(TrajectoryResult result, String filename) throws IOException {
        FileWriter writer = new FileWriter(filename);
        for(LineResult r:result.getLines()){
            String sb = r.getLineID() + "," +
                    r.getRoadID() + "," +
                    r.getStartTime() + "," +
                    r.getEndTime() + "," +
                    r.getMeanSpeed() + "," +
                    r.getStartSpeed() + "," +
                    r.getEndSpeed() + "\n";
            writer.write(sb);
        }
        writer.flush();
        writer.close();
    }
    public static void export(List<String> result, String filename) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        for(String c:result){
            writer.write(c);
            writer.newLine();
        }
        writer.flush();
        writer.close();
    }
    public static void export(List<List<String>> result, String path, String[] names) throws IOException {
        int i = 0;
        for(List<String> item:result){
            String name = names == null?String.valueOf(i++):names[(i++)/names.length];
            export(item, path+"//"+name+".txt");
        }
    }
}
