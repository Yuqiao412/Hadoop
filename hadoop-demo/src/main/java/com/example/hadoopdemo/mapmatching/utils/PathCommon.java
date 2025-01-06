package com.example.hadoopdemo.mapmatching.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.example.hadoopdemo.mapmatching.a.Graph;
import com.example.hadoopdemo.mapmatching.a.Vertex;
import com.example.hadoopdemo.mapmatching.bean.*;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.jdbc.CRUD;
import com.example.hadoopdemo.mapmatching.match.Common;

import java.util.List;
import java.util.Map;

/**
 * Created by AJQK on 2020/1/12.
 */
public class PathCommon {

    public static void addVertex(Graph graph, Point start, Point stop, double distance, int pathClass) {

        Vertex StartVertex = graph.getVertexByID(start.getNodeID());
        if(StartVertex==null){
            StartVertex=new Vertex(start);
            graph.addVertex(StartVertex);
        }
        Vertex StopVertex=graph.getVertexByID(stop.getNodeID());
        if(StopVertex==null){

            StopVertex=new Vertex(stop);
            graph.addVertex(StopVertex);
        }

        Triple<Vertex, Double, Integer> adjacency = new Triple<>(StopVertex, distance, pathClass);
        StartVertex.addAdjacency(adjacency);
    }

    public static void addVertexWithCheck(Graph graph, Point start, Point stop, double distance, int pathClass) {

        Vertex StartVertex=graph.getVertexByID(start.getNodeID());
        if(StartVertex==null){
            StartVertex=new Vertex(start);
            graph.addVertex(StartVertex);
        }
        Vertex StopVertex=graph.getVertexByID(stop.getNodeID());
        if(StopVertex==null){

            StopVertex=new Vertex(stop);
            graph.addVertex(StopVertex);
        }

        Triple<Vertex, Double, Integer> adjacency = new Triple<>(StopVertex, distance, pathClass);
        StartVertex.addAdjacencyWithCheck(adjacency);
    }

    public static Graph initGraph() throws Exception{
        Graph graph=new Graph();
        String sql="select roadid, start, stop, direction, distance, pathclass, ST_AsGeoJSON(a.point) as startpoint, ST_AsGeoJSON(b.point) as stoppoint "
                + "from roads, nodes a, nodes b "
                + "where start = a.nodeid and stop = b.nodeid";//start 方向是不是有问题
        List<Map<String, Object>> nodes = CRUD.read(sql, null);
        addNoteToGraph(graph,nodes);
        return graph;
    }

    public static void addNoteToGraph(Graph graph, List<Map<String, Object>> nodes) throws Exception {
        for(Map<String, Object> node:nodes){
            int startId = Integer.parseInt(node.get("start").toString());
            int stopId = Integer.parseInt(node.get("stop").toString());
            JSONArray coor1 = JSON.parseObject(node.get("startpoint").toString()).getJSONArray("coordinates");
            double x1 = coor1.getDoubleValue(0);
            double y1 = coor1.getDoubleValue(1);
            Point startP = new Point(x1, y1);
            startP.setNodeID(startId);


            JSONArray coor2 = JSON.parseObject(node.get("stoppoint").toString()).getJSONArray("coordinates");
            double x2 = coor2.getDoubleValue(0);
            double y2 = coor2.getDoubleValue(1);
            Point stopP = new Point(x2, y2);
            stopP.setNodeID(stopId);

            double dis = Double.parseDouble(node.get("distance").toString());
            int direction = Integer.parseInt(node.get("direction").toString());
            Object oPathClass = node.get("pathclass");
            int pathClass = oPathClass==null?-1:Integer.parseInt(oPathClass.toString());
            switch (direction) {//0未分类，1双向通行，2正向，3反向，4禁行
                case 0:
                case 1:
                    PathCommon.addVertex(graph, startP, stopP, dis, pathClass);
                    PathCommon.addVertex(graph, stopP, startP, dis, pathClass);
                    break;
                case 2:
                    PathCommon.addVertex(graph, startP, stopP, dis, pathClass);
                    break;
                case 3:
                    PathCommon.addVertex(graph, stopP, startP, dis, pathClass);
                    break;
            }
        }
    }
    // 采用时分秒 可用
//    public static long getSecondGap(long former, long later){
//        long time = Transform.Time2Second(later)- Transform.Time2Second(former);
//        if(time<0) time += 86400;
//        return time;
//    }

    public static long getSecondGap(long former, long later){
       return (later-former)/1000;
    }
    public static int getSpeedByPathClass(int pathclass){
        int kmh;
        switch (pathclass){
            case 0:
                kmh = 40;
                break;
            case 15:
                kmh =  30;
                break;
            case 3:
                kmh =  80;
                break;
            case 4:
                kmh =  100;
                break;
            case 2:
            default:
                kmh =  60;
                break;
        }
        return kmh*1000/3600;
    }
    public static double getDistanceInLine(Point p, int lineID, int roadID){
        List<Line> lines = RoadData.getLinesByRoad(roadID);
        double distance = 0;
        Line cur = null;
        for(Line line:lines){
            // 20200616代码提前
            if(line.getLineid() == lineID) {
                cur = line;
                break;
            }
            distance+=Transform.getDistance(Transform.lonLat2Mercator(line.getStart()),Transform.lonLat2Mercator(line.getEnd()));
//            if(line.getLineid() == lineID)
//                break;
        }
        if(cur!=null)
            distance+=Transform.getDistance(Transform.lonLat2Mercator(cur.getStart()),Transform.lonLat2Mercator(p));
        return distance;
    }
    public static Pair<Pair<Integer, Double>, Pair<Integer, Double>> getODAndDistance(Point p){
        List<Line> lines = RoadData.getLinesByRoad(p.getRoadID());
        double distance = 0;
        Line cur = null;
        double startDis = 0;
        for(Line line:lines){
            if(line.getLineid() == p.getLineID()) {
                cur = line;
                startDis = distance+Transform.getDistance(Transform.lonLat2Mercator(cur.getStart()),Transform.lonLat2Mercator(p));
            }
            distance+=Transform.getDistance(Transform.lonLat2Mercator(line.getStart()),Transform.lonLat2Mercator(line.getEnd()));
        }
        double endDis = distance - startDis;
        Road road = RoadData.getRoadByID(p.getRoadID());
        int startId = road.getStart();
        int endId = road.getStop();
        Pair<Integer, Double> realStart, realEnd;

        assert cur != null;
        int direction = cur.getDirection();
        switch (direction) {
            case 2://正向
                realStart = new Pair<>(startId, startDis);
                realEnd = new Pair<>(endId, endDis);
                break;
            case 3://反向
                realStart = new Pair<>(endId, endDis);
                realEnd = new Pair<>(startId, startDis);
                break;
            case 0:
            case 1:
                Point point_min = cur.getStart();
                Point point_max = cur.getEnd();
                float lineAngle1 = Common.CalculateAngle(point_min.getX(), point_min.getY(), point_max.getX(), point_max.getY());
                float angleDeviation1 = Common.CalculateAngleDeviationWithoutConsideringTwoWay(direction, p.getAngle(), lineAngle1);
                float lineAngle2 = Common.CalculateAngle(point_max.getX(), point_max.getY(), point_min.getX(), point_min.getY());
                float angleDeviation2 = Common.CalculateAngleDeviationWithoutConsideringTwoWay(direction, p.getAngle(), lineAngle2);
                if (angleDeviation1 < angleDeviation2) {
                    realStart = new Pair<>(startId, startDis);
                    realEnd = new Pair<>(endId, endDis);
                } else {
                    realStart = new Pair<>(endId, endDis);
                    realEnd = new Pair<>(startId, startDis);
                }
                break;
            default:
                return null;
        }
        return new Pair<>(realStart, realEnd);
    }

}
