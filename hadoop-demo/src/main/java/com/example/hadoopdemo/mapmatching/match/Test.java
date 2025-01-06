package com.example.hadoopdemo.mapmatching.match;

import com.example.hadoopdemo.mapmatching.HMM.Matching;
import com.example.hadoopdemo.mapmatching.a.Search;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.bean.TrajectoryResult;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.jdbc.CRUD;

import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Test {
    public static void main(String[] args){
        try {
            RoadData.init();
            Search.initGraph();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int vehicleID = 16941;
        try {
            List<Map<String, Object>> data = CRUD.read("select * from data_low where vehicle = ? and time > ? and time < ? order by time",
                    new Object[]{vehicleID, 191900, 192300});
            List<Point> points = new LinkedList<>();
            for(Map<String, Object> map:data) {
                Point p = new Point(Double.parseDouble(map.get("x").toString()),
                        Double.parseDouble(map.get("y").toString()),
                        Integer.parseInt(map.get("angle").toString()),
                        Integer.parseInt(map.get("time").toString()),
                        0,0);
                points.add(p);
            }
            TrajectoryResult result = Matching.restoration(points);
//            TrajectoryResult result = RouteRestore.Restore(points);

            String filename = "C:\\Users\\liboz\\Desktop\\"+vehicleID+".txt";
            FileWriter writer = new FileWriter(filename);

            for(Point p:result.getPoints()){
                String sb = p.getX() + "," +
                        p.getY() + "," +
                        p.getAngle() + "," +
                        vehicleID + "\n";
                writer.write(sb);
            }
            writer.flush();
            writer.close();
            System.out.println("pause");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
