package com.example.hadoopdemo.mapmatching.data;

import com.example.hadoopdemo.mapmatching.bean.IntegerPair;
import com.example.hadoopdemo.mapmatching.bean.Line;
import com.example.hadoopdemo.mapmatching.bean.Node;
import com.example.hadoopdemo.mapmatching.bean.Road;
import com.example.hadoopdemo.mapmatching.jdbc.CRUD;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by AJQK on 2019/12/27.
 */
public class RoadData {
    private static boolean isInit = false;
    private static Map<Integer, Node> nodeIndex = new TreeMap<>();
    private static Map<Integer, Road> roadIndex = new TreeMap<>();
    private static Map<Integer, Line> lineIndex = new TreeMap<>();
    private static Map<Integer, List<Line>> linesIndex = new TreeMap<>();
    private static Map<IntegerPair,Integer> roadNodesIndex = new TreeMap<>();
//    private static RStarTree<Line> lineTrees = new RStarTree<>();
    private static STRtree lineTrees = new STRtree();
    // 根据节点找路
    private static Map<Integer,List<Road>> roadsIndex = new TreeMap<>();


    public static void init(){
        if(isInit)
            return;
        try {
            List<Node> nodes =  CRUD.read("select * from nodes order by nodeid",null, Node.class);
            for(Node node : nodes){
                nodeIndex.put(node.getNodeid(), node);
            }
            List<Road> roads =  CRUD.read("select * from roads order by roadid",null, Road.class);
            for(Road road : roads){
                roadIndex.put(road.getRoadid(), road);
                roadNodesIndex.put(new IntegerPair(road.getStart(),road.getStop()),road.getRoadid());
                addRoadByNode(road,road.getStart());
                addRoadByNode(road,road.getStop());
            }
            List<Line> lines =  CRUD.read("select * from lines order by lineid",null, Line.class);
//            int roadid = -1;
//            List<Line> tmpLines = new ArrayList<>();
            for(Line line : lines){
                Envelope envelope = new Envelope(line.getStart().getX(),line.getEnd().getX(),
                        line.getStart().getY(), line.getEnd().getY());
                lineTrees.insert(envelope, line);
                lineIndex.put(line.getLineid(),line);
                List<Line> lineList;
                if(linesIndex.containsKey(line.getRoadid())){
                    lineList = linesIndex.get(line.getRoadid());
                }else{
                    lineList = new ArrayList<>();
                    linesIndex.put(line.getRoadid(), lineList);
                }
                lineList.add(line);
//                if(line.getRoadid()!=roadid){
//                    if(tmpLines.size()>0)
//                        linesIndex.put(roadid,tmpLines);
//                    tmpLines = new ArrayList<>();
//                    roadid = line.getRoadid();
//                }
//                tmpLines.add(line);
            }
//            if(tmpLines.size()>0)
//                linesIndex.put(roadid,tmpLines);
            lineTrees.build();
            isInit=true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static Line getLineByID(int id){
        return lineIndex.get(id);
    }
    public static Road getRoadByID(int id){
        return roadIndex.get(id);
    }
    public static Node getNodeByID(int id){
        return nodeIndex.get(id);
    }
    public static Integer getRoadByNodes(int start, int end){
        return roadNodesIndex.get(new IntegerPair(start, end));
    }
    public static List<Line> getLinesByRoad(int id){
        return linesIndex.get(id);
    }
    @SuppressWarnings("unchecked")
    public static List<Line> getLinesByExtent(double xmin, double xmax, double ymin, double ymax){
        return lineTrees.query(new Envelope(xmin, xmax, ymin, ymax));
    }
//    public static List<Node> getNodesByExtent(Rectangle rectangle){
//        return pointTrees.searchData(rectangle);
//    }
    public static void addRoadByNode(Road road, int nodeID){
        List<Road> roads;
        if(roadsIndex.containsKey(nodeID)){
            roads = roadsIndex.get(nodeID);
        }else{
            roads = new ArrayList<>();
            roadsIndex.put(nodeID,roads);
        }
        roads.add(road);
    }
    public static List<Road> getRoadsByNode(int nodeID){
        return roadsIndex.get(nodeID);
    }

}
