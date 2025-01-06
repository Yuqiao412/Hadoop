package com.example.hadoopdemo.mapmatching.HMM;

import com.example.hadoopdemo.mapmatching.HMM.bean.CandidatePoint;
import com.example.hadoopdemo.mapmatching.HMM.bean.DriveSegment;
import com.example.hadoopdemo.mapmatching.a.ASparkResult;
import com.example.hadoopdemo.mapmatching.a.Search;
import com.example.hadoopdemo.mapmatching.a.Vertex;
import com.example.hadoopdemo.mapmatching.bean.*;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.match.Common;
import com.example.hadoopdemo.mapmatching.utils.PathCommon;
import com.example.hadoopdemo.mapmatching.utils.Transform;

import java.util.*;

public class CommonMethod {
    public static float getAngleDeviation(Point current, Line line){
        float lineAngle = Common.CalculateAngle(line.getStart().getX(),
                line.getStart().getY(),
                line.getEnd().getX(),
                line.getEnd().getY());
        return Common.CalculateAngleDeviationByDirection(line.getDirection(), current.getAngle(), lineAngle);
    }
    public static Pair<Float, CandidatePoint> CalculatePointWithAllParams(Point current, Line line){
        // AJQK 我加了一个判断满足角度的候选路段
//        int lineAngle = Common.CalculateAngle(line.getStart().getX(),
//                line.getStart().getY(),
//                line.getEnd().getX(),
//                line.getEnd().getY());
//        int deviation = Common.CalculateAngleDeviationByDirection(line.getDirection(), current.getAngle(), lineAngle);
        float deviation = getAngleDeviation(current,line);
//        // 后期计算概率的时候会继续用角度 这里放开一些
//        if(deviation>35)
//            return null;
        CandidatePoint result = new CandidatePoint();
        result.setLine(line);
        result.setSamplingPoint(current);
        double r = Common.CalculateR(current, line.getStart().getX(), line.getStart().getY(), line.getEnd().getX(), line.getEnd().getY());
        Point p;
        if (r < 0)
            p = new Point(line.getStart().getX(),line.getStart().getY());
        else if (r > 1)
            p = new Point(line.getEnd().getX(),line.getEnd().getY());
        else{
            if(line.getStart().getX() == line.getEnd().getX()){
                p = new Point(line.getStart().getX(), current.getY());
            }
            else{
                double xr=line.getStart().getX() + r * (line.getEnd().getX() - line.getStart().getX());
                double yr=line.getStart().getY() + r * (line.getEnd().getY() - line.getStart().getY());
                p = new Point(xr, yr);
            }
        }
        p.setLineID(line.getLineid());
        p.setRoadID(line.getRoadid());
        p.setAngle(current.getAngle());
        p.setStatus(current.getStatus());
        p.setTime(current.getTime());
        p.setVelocity(current.getVelocity());
        result.setR(r);
        result.setCandidatePoint(p);
        result.setHeight(Transform.getDistance(Transform.lonLat2Mercator(current),Transform.lonLat2Mercator(result.getCandidatePoint())));
        result.setScale(PathCommon.getDistanceInLine(p,line.getLineid(),line.getRoadid())/ RoadData.getRoadByID(line.getRoadid()).getDistance());
        return new Pair<>(deviation,result);
    }
    public static Pair<Float, CandidatePoint> CalculatePoint4Parallel(Point current, Line line){
        float deviation = getAngleDeviation(current,line);
        CandidatePoint result = new CandidatePoint();
        result.setLine(line);
        result.setSamplingPoint(current);
        double r = Common.CalculateR(current, line.getStart().getX(), line.getStart().getY(), line.getEnd().getX(), line.getEnd().getY());
        Point p;
        if (r < 0)
            p = new Point(line.getStart().getX(),line.getStart().getY());
        else if (r > 1)
            p = new Point(line.getEnd().getX(),line.getEnd().getY());
        else{
            if(line.getStart().getX() == line.getEnd().getX()){
                p = new Point(line.getStart().getX(), current.getY());
            }
            else{
                double xr=line.getStart().getX() + r * (line.getEnd().getX() - line.getStart().getX());
                double yr=line.getStart().getY() + r * (line.getEnd().getY() - line.getStart().getY());
                p = new Point(xr, yr);
            }
        }
        p.setLineID(line.getLineid());
        p.setRoadID(line.getRoadid());
        p.setAngle(current.getAngle());
        result.setR(r);
        result.setCandidatePoint(p);
        result.setHeight(Transform.getDistance(Transform.lonLat2Mercator(current),Transform.lonLat2Mercator(result.getCandidatePoint())));
        return new Pair<>(deviation,result);
    }

    /**
     * former为true获取当前路的起点及关联的连通路，false去当前路的重点及关联的联通路 null两者都取
     */
    private static List<List<Integer>> getAllRoadIDs(Line line, Boolean former){
        List<Line> lines = RoadData.getLinesByRoad(line.getRoadid());
        int index = -1;
        for(int i=0;i<lines.size();i++){
            if(lines.get(i).getLineid() == line.getLineid()){
                index = i;
                break;
            }
        }
        if(index==-1)
            return null;
        Road road = RoadData.getRoadByID(line.getRoadid());
        List<List<Integer>> result = new ArrayList<>();
        Set<Integer> all = new HashSet<>();
        List<Integer> related = getMergedRoad(road.getRoadid());
        boolean border = former!=null && ((former && index==0) || (!former && index == lines.size()-1));
        if(border){
            int nodeId = former?road.getStart():road.getStop();
            getAllMergedRoads(nodeId,road.getRoadid(),all);
        }else{
            getAllMergedRoads(road.getStart(),road.getRoadid(),all);
            getAllMergedRoads(road.getStop(),road.getRoadid(),all);
        }
        all.addAll(related);
        result.add(Arrays.asList(all.toArray(new Integer[0])));
        result.add(related);
        return result;
    }
    /**
     * former为true获取当前路的起点及关联的连通路，false为当前路的终点及关联的联通路 null只取连通路
     */
    private static List<Integer> getAllRoadIDs2(Line line, Boolean former){
        List<Line> lines = RoadData.getLinesByRoad(line.getRoadid());
        int index = -1;
        for(int i=0;i<lines.size();i++){
            if(lines.get(i).getLineid() == line.getLineid()){
                index = i;
                break;
            }
        }
        if(index==-1)
            return null;
        List<Integer> related = getMergedRoad(line.getRoadid());
        if(former==null){
//            p.setNodeID(-1);
            return related;
        }
        Set<Integer> all = new HashSet<>();
        boolean border = (former && index==0) || (!former && index == lines.size()-1);
        if(border){
            Road road = RoadData.getRoadByID(line.getRoadid());
            int nodeId = former?road.getStart():road.getStop();
//            p.setNodeID(nodeId);
            getAllMergedRoads(nodeId,road.getRoadid(),all);
        }else{
//            getAllMergedRoads(road.getStart(),road.getRoadid(),all);
//            getAllMergedRoads(road.getStop(),road.getRoadid(),all);
//            p.setNodeID(-1);
            return related;
        }
        all.addAll(related);
        return new ArrayList<>(Arrays.asList(all.toArray(new Integer[0])));
    }
    // 为了放开限制的多路径输出加的新函数
    private static List<List<Integer>> getAllRoadIDs(Line line, Boolean former,boolean edge){
        List<Line> lines = RoadData.getLinesByRoad(line.getRoadid());
        int index = -1;
        for(int i=0;i<lines.size();i++){
            if(lines.get(i).getLineid() == line.getLineid()){
                index = i;
                break;
            }
        }
        if(index==-1)
            return null;
        Road road = RoadData.getRoadByID(line.getRoadid());
        List<List<Integer>> result = new ArrayList<>();
        Set<Integer> all = new HashSet<>();
        List<Integer> related = getMergedRoad(road.getRoadid());
        boolean border = former!=null && ((former && index==0) || (!former && index == lines.size()-1));
        if(border){
            int nodeId = former?road.getStart():road.getStop();
            getAllMergedRoads(nodeId,road.getRoadid(),all);
        }
        // 20200819 如果是普通路加上 端点的扫描 小交点（形状点）不需要
        else if(former==null || edge){
            getAllMergedRoads(road.getStart(),road.getRoadid(),all);
            getAllMergedRoads(road.getStop(),road.getRoadid(),all);
        }
        all.addAll(related);
        result.add(Arrays.asList(all.toArray(new Integer[0])));
        result.add(related);
        return result;
    }
    private static void getAllMergedRoads(int nodeID, int roadID, Set<Integer> all){
        List<Road> roads = RoadData.getRoadsByNode(nodeID);
        for(Road r:roads){
            if(r.getRoadid() == roadID){
                continue;
            }
            List<Integer> mergedRoads = getMergedRoad(r);
            all.addAll(mergedRoads);
        }
    }
    public static List<Integer> getMergedRoad(Integer roadID){
        Road road = RoadData.getRoadByID(roadID);
        return getMergedRoad(road);
    }
    private static List<Integer> getMergedRoad(Road road){
        Set<Integer> roadsSet = new HashSet<>();
        Queue<Integer> Open = new LinkedList<>();
        Set<Integer> Close = new HashSet<>();
        Open.add(road.getStart());
        Open.add(road.getStop());
        roadsSet.add(road.getRoadid());
        while(!Open.isEmpty()){
            int nodeID = Open.poll();
            Close.add(nodeID);
            List<Road> roads = RoadData.getRoadsByNode(nodeID);
            if(roads.size()==1){
                roadsSet.add(roads.get(0).getRoadid());
            }else if(roads.size()==2){
                for(Road r:roads){
                    roadsSet.add(r.getRoadid());
                    if(!Close.contains(r.getStart())){
                        Open.offer(r.getStart());
                    }
                    if(!Close.contains(r.getStop())){
                        Open.offer(r.getStop());
                    }
                }
            }
        }
        return Arrays.asList(roadsSet.toArray(new Integer[0]));
    }
    public static CandidatePoint CalculatePoint(Point current, Line line){
        // 以下原逻辑
//        CandidatePoint result = new CandidatePoint();
//        result.setLine(line);
//        result.setSamplingPoint(current);
//        double r = Common.CalculateR(current, line.getStart().getX(), line.getStart().getY(), line.getEnd().getX(), line.getEnd().getY());
//        Point p;
//        if (r < 0)
//            p = new Point(line.getStart().getX(),line.getStart().getY());
//        else if (r > 1)
//            p = new Point(line.getEnd().getX(),line.getEnd().getY());
//        else{
//            if(line.getStart().getX() == line.getEnd().getX()){
//                p = new Point(line.getStart().getX(), current.getY());
//            }
//            else{
//                double xr=line.getStart().getX() + r * (line.getEnd().getX() - line.getStart().getX());
//                double yr=line.getStart().getY() + r * (line.getEnd().getY() - line.getStart().getY());
//                p = new Point(xr, yr);
//            }
//        }
//        p.setLineID(line.getLineid());
//        p.setRoadID(line.getRoadid());
//        result.setCandidatePoint(p);
//        result.setHeight(Transform.getDistance(Transform.lonLat2Mercator(current),Transform.lonLat2Mercator(result.getCandidatePoint())));
//        result.setScale(PathCommon.getDistanceInLine(p,line.getLineid(),line.getRoadid())/ RoadData.getRoadByID(line.getRoadid()).getDistance());
//        return result;
        return CalculatePointWithAllParams(current, line).getB();
    }
    // 时空分析
    public static double spatialTemporalAnalysis(CandidatePoint p1, CandidatePoint p2,double mu,double sigma) throws Exception {
        //spatial
        double n = gaussianFunction(p2.getHeight(),mu,sigma);
        double d = Transform.getEuclideanDistance(p1.getSamplingPoint(),p2.getSamplingPoint());
        List<DriveSegment> segments = shortestPath(p1, p2);
        double w = shortestPathLength(segments);
        if(w==0||w==Double.NEGATIVE_INFINITY)
            return Double.NEGATIVE_INFINITY;
        w=w==Double.POSITIVE_INFINITY?1:w;
        double fs = d<=w?n*d/w:n*w/d;//20200619加个判断 这个距离最大是1
        //temporal
        double meanSpeed = w / PathCommon.getSecondGap(p1.getSamplingPoint().getTime(),p2.getSamplingPoint().getTime());
        double ft = temporalAnalysis(segments, meanSpeed);
        return fs==Double.NEGATIVE_INFINITY?fs:fs*ft;
    }
    // 时空分析20200710 避免最短路径远大于两点之间距离
    public static Pair<Double,Boolean> spatialTemporalAnalysis2(CandidatePoint p1, CandidatePoint p2,double mu,double sigma) throws Exception {
        //spatial
        double d = Transform.getEuclideanDistance(p1.getSamplingPoint(),p2.getSamplingPoint());
        List<DriveSegment> segments = shortestPath(p1, p2);
        double w = shortestPathLength(segments);
        boolean segment = segments==null || w==Double.POSITIVE_INFINITY || w<=d*1.5;//20201202备注 || segments.size()<3
        if(w==0||w==Double.NEGATIVE_INFINITY)
            return new Pair<>(Double.NEGATIVE_INFINITY,segment);
//        double ratio = d<=w?d/w:w/d;
        // 20200713 前后匹配到同一个点的话 这样不会被干掉
        w=w==Double.POSITIVE_INFINITY?1:w;
        if(w>50 && w>d*10){
            return new Pair<>(Double.NEGATIVE_INFINITY,segment);
        }
        double n = gaussianFunction(p2.getHeight(),mu,sigma);
        double fs = n*(d<=w?d/w:w/d);//20200619加个判断 这个距离最大是1
        //temporal
        float timeGap = PathCommon.getSecondGap(p1.getSamplingPoint().getTime(),p2.getSamplingPoint().getTime());
        double meanSpeed = w / timeGap;
        double ft = timeGap==0?1:temporalAnalysis(segments, meanSpeed);
        return new Pair<>(fs==Double.NEGATIVE_INFINITY?fs:fs*ft,segment);
    }
    // 20201112 返回距离 尝试距离标准化
    public static Pair<Double,Double> spatialTemporalAnalysis3(CandidatePoint p1, CandidatePoint p2,double mu,double sigma) throws Exception {
        //spatial
        double d = Transform.getEuclideanDistance(p1.getSamplingPoint(),p2.getSamplingPoint());
        List<DriveSegment> segments = shortestPath(p1, p2);
        double w = shortestPathLength(segments);
        if(w==0||w==Double.NEGATIVE_INFINITY)
            return new Pair<>(Double.NEGATIVE_INFINITY,w);
        // 20200713 前后匹配到同一个点的话 这样不会被干掉
        w=w==Double.POSITIVE_INFINITY?1:w;
        // 20210320 重新加上 顺便看看论文中写了吗
        if(w>50 && w>d*3){
            return new Pair<>(Double.NEGATIVE_INFINITY,w);
        }
        double n = gaussianFunction(p2.getHeight(),mu,sigma);
        //temporal
        float timeGap = PathCommon.getSecondGap(p1.getSamplingPoint().getTime(),p2.getSamplingPoint().getTime());
        double meanSpeed = w / timeGap;
        double ft = timeGap==0?1:temporalAnalysis(segments, meanSpeed);
        return new Pair<>(n==Double.NEGATIVE_INFINITY?n:n*ft,w);
    }
    // 20210307 返回距离 尝试距离标准化 返回道路信息
    public static Triple<Double,Double,List<DriveSegment>> spatialTemporalAnalysis3WithSegment(CandidatePoint p1, CandidatePoint p2,double mu,double sigma) throws Exception {
        //spatial
        double d = Transform.getEuclideanDistance(p1.getSamplingPoint(),p2.getSamplingPoint());
        List<DriveSegment> segments = shortestPathWithShapePoints(p1, p2);
        double w = shortestPathLength(segments);
        if(w==0||w==Double.NEGATIVE_INFINITY)
            return new Triple<>(Double.NEGATIVE_INFINITY,w,segments);
        // 20200713 前后匹配到同一个点的话 这样不会被干掉
        w=w==Double.POSITIVE_INFINITY?1:w;
        // 20210320 重新加上 顺便看看论文中写了吗
        if(w>50 && w>d*3){
            return new Triple<>(Double.NEGATIVE_INFINITY,w,segments);
        }
        double n = gaussianFunction(p2.getHeight(),mu,sigma);
        //temporal
        float timeGap = PathCommon.getSecondGap(p1.getSamplingPoint().getTime(),p2.getSamplingPoint().getTime());
        double meanSpeed = w / timeGap;
        double ft = timeGap==0?1:temporalAnalysis(segments, meanSpeed);
        return new Triple<>(n==Double.NEGATIVE_INFINITY?n:n*ft,w,segments);
    }

    public static double gaussianFunction(double distance, double mu, double sigma){
        return Math.exp(-Math.pow(distance-mu,2)/(2*Math.pow(sigma,2)))/(Math.sqrt(2*Math.PI)*sigma);
    }
    public static double shortestPathLength(List<DriveSegment> segments){
        if(segments==null)
            return Double.NEGATIVE_INFINITY;
        double w = 0;
        for(DriveSegment segment:segments){
            w+=segment.getLength();
        }
        return segments.size()==0?Double.NEGATIVE_INFINITY:(w==0?Double.POSITIVE_INFINITY:w);
    }
    private static double temporalAnalysis(List<DriveSegment> segments,double meanSpeed){
        // 原文公式有点不太合理 整体路段的平均速度应该就有一个 按照原文先写着
        if(segments==null)
            return Double.NEGATIVE_INFINITY;
        double a = 0, b =0, c=0;// a是分子，b是分母的左半部分，c是分母的右半部分
        for(DriveSegment segment:segments){
            a+=segment.getSpeed()*meanSpeed;
            b+=Math.pow(segment.getSpeed(),2);
            c+=Math.pow(meanSpeed,2);
        }
        return a/Math.sqrt(b)/Math.sqrt(c);
    }
    public static List<DriveSegment> shortestPath(CandidatePoint p1, CandidatePoint p2) throws Exception {
        List<DriveSegment> result = new LinkedList<>();
        Point start = GetOorD(p1,true);
        Point end = GetOorD(p2,false);
        if(start ==null || end == null)
            return null;
        if(start.getRoadID()==end.getRoadID()){
//            addDriveSegment(start,p2.getScale()-p1.getScale(),result);
            Road road = RoadData.getRoadByID(start.getRoadID());
            if(road.getDirection()==4 ||
                    (road.getDirection()==2 && p2.getScale()<p1.getScale()) ||
                    (road.getDirection()==3 && p2.getScale()>p1.getScale()))
                return null;
            result.add(new DriveSegment(road.getRoadid(),road.getDistance()*Math.abs(p2.getScale()-p1.getScale()),road.getSpeed()*1000/3600));
            return result;
        }

//        List<Line> startShapePoints = RouteRestore.getStartShapePoint(start.getLineID(), start.getRoadID(), start.getNodeID());
//        List<Line> stopShapePoints = RouteRestore.getStopShapePoint(end.getLineID(), end.getRoadID(), end.getNodeID());
        // 添加p1到最短路径起点的长度
//        Road startRoad = RoadData.getRoadByID(start.getRoadID());
//        double startLength = startRoad.getStart() == start.getNodeID()?startRoad.getDistance()*p1.getScale():startRoad.getDistance()*(1-p1.getScale());
//        result.add(new DriveSegment(startRoad.getRoadid(),startLength,(float)startRoad.getSpeed()*1000/3600));
//        Road endRoad = RoadData.getRoadByID(end.getRoadID());
//        double endLength = endRoad.getStart() == end.getNodeID()?endRoad.getDistance()*p1.getScale():endRoad.getDistance()*(1-p1.getScale());
//        result.add(new DriveSegment(endRoad.getRoadid(),endLength,(float)endRoad.getSpeed()*1000/3600));
        addDriveSegment(start,p1.getScale(),result);
        addDriveSegment(end,p2.getScale(),result);
        if(start.getNodeID() == end.getNodeID()){
            return result;
        }

        Vertex startNode=new Vertex(start);
        Vertex endNode=new Vertex(end);
        long time = PathCommon.getSecondGap(p1.getSamplingPoint().getTime(), p2.getSamplingPoint().getTime());
        //加上time 保证每个点的效率！！
        ASparkResult<Vertex> aSparkResult = Search.searchNodeAndSolve(startNode, endNode, null, time);//A*
        if(aSparkResult==null)
            return null;
        Vertex last = null;
        for(Vertex vertex:aSparkResult.getRoute()){
            if(last != null){
                Integer roadid = RoadData.getRoadByNodes(last.getVertexID(),vertex.getVertexID());
//                boolean direction = true; //正向
                if(roadid==null){
                    roadid = RoadData.getRoadByNodes(vertex.getVertexID(),last.getVertexID());
//                    direction = false;
                }
                Road road = RoadData.getRoadByID(roadid);
                // 应该不存在有roadid但是没有road的情况吧 20210308
//                if(road == null && direction){ // 这里应该是写反了 !direction->direction
//                    roadid = RoadData.getRoadByNodes(vertex.getVertexID(),last.getVertexID());
//                    road = RoadData.getRoadByID(roadid);
//                }
                if(road != null){
                    result.add(new DriveSegment(roadid,road.getDistance(),road.getSpeed()*1000/3600));
                }
            }
            last = vertex;
        }
        return result;
    }
    private static void addDriveSegment(Point node,double scale,List<DriveSegment> result){
        Road road = RoadData.getRoadByID(node.getRoadID());
        double startLength = road.getStart() == node.getNodeID()?road.getDistance()*scale:road.getDistance()*(1-scale);
        startLength=startLength<0?0:startLength;//20210320 double可能存在精度误差
        result.add(new DriveSegment(road.getRoadid(),startLength,road.getSpeed()*1000/3600));
    }
    public static Point GetOorD(CandidatePoint p,boolean isStart){
        Line line = RoadData.getLineByID(p.getCandidatePoint().getLineID());
        if(line!=null){
            int roadId = line.getRoadid();
            boolean flag;
            if(p.getScale()<=0||p.getScale()>=1){
                flag = p.getScale()<=0;
            }else{
                int direction = line.getDirection();
                Point pointMin=line.getStart();
                Point pointMax=line.getEnd();
                switch (direction) {
                    case 2://正向
                        flag = !isStart;
                        break;
                    case 3://反向
                        flag = isStart;
                        break;
                    case 0:
                    case 1:
                        float lineAngle1 = Common.CalculateAngle(pointMin.getX(),pointMin.getY(),pointMax.getX(),pointMax.getY());
                        float angleDeviation1 = Common.CalculateAngleDeviationWithoutConsideringTwoWay(direction, p.getCandidatePoint().getAngle(), lineAngle1);
                        float lineAngle2 = Common.CalculateAngle(pointMax.getX(),pointMax.getY(),pointMin.getX(),pointMin.getY());
                        float angleDeviation2 = Common.CalculateAngleDeviationWithoutConsideringTwoWay(direction, p.getCandidatePoint().getAngle(), lineAngle2);
                        flag = angleDeviation1 < angleDeviation2 != isStart;
                        break;
                    default:
                        return null;
                }
            }
            Point target = getNodeIDByRoad(roadId, flag);
            target.setLineID(p.getCandidatePoint().getLineID());//存放的待匹配点的线id
            target.setRoadID(roadId);
            return target;
        }
        return null;
    }
    private static Point getNodeIDByRoad(int roadID,boolean isStart){
        Road road =RoadData.getRoadByID(roadID);
        Node node = isStart?RoadData.getNodeByID(road.getStart()):RoadData.getNodeByID(road.getStop());
        Point target = new Point(node.getX(),node.getY());
        target.setNodeID(node.getNodeid());
        target.setRoadID(roadID);
        return target;
    }
    public static List<DriveSegment> shortestPathWithShapePoints(CandidatePoint p1, CandidatePoint p2) throws Exception {
        List<DriveSegment> result = new LinkedList<>();
        Point start = GetOorD(p1,true);
        Point end = GetOorD(p2,false);
        if(start ==null || end == null)
            return null;
        if(start.getRoadID()==end.getRoadID()){
//            addDriveSegment(start,p2.getScale()-p1.getScale(),result);
            Road road = RoadData.getRoadByID(start.getRoadID());
            if(road.getDirection()==4 ||
                    (road.getDirection()==2 && p2.getScale()<p1.getScale()) ||
                    (road.getDirection()==3 && p2.getScale()>p1.getScale()))
                return null;
            // 获取line
            List<Line> lines = RoadData.getLinesByRoad(start.getRoadID());
            int index = -1;
            int startIndex = -1, stopIndex = -1;
            for(Line line:lines){
                ++index;
                if(line.getLineid() == start.getLineID()){
                    startIndex=index;
                    if(stopIndex!=-1)
                        break;
                }
                if(line.getLineid() == end.getLineID()) {
                    stopIndex = index;
                    if (startIndex != -1)
                        break;
                }
            }
            if(startIndex == -1 || stopIndex == -1)
                System.out.println("pause");
            List<Line> curChosen = startIndex<=stopIndex?lines.subList(startIndex,stopIndex+1):lines.subList(stopIndex,startIndex+1);
            result.add(new DriveSegment(road.getRoadid(),road.getDistance()*Math.abs(p2.getScale()-p1.getScale()),road.getSpeed()*1000/3600,curChosen,startIndex<=stopIndex));
            return result;
        }

        addDriveSegmentWithShapePoints(start,p1.getScale(),result,true);
        if(start.getNodeID() == end.getNodeID()){
            addDriveSegmentWithShapePoints(end,p2.getScale(),result,false);
            return result;
        }

        Vertex startNode=new Vertex(start);
        Vertex endNode=new Vertex(end);
        long time = PathCommon.getSecondGap(p1.getSamplingPoint().getTime(), p2.getSamplingPoint().getTime());
        //加上time 保证每个点的效率！！
        ASparkResult<Vertex> aSparkResult = Search.searchNodeAndSolve(startNode, endNode, null, time);//A*
        if(aSparkResult==null)
            return null;
        Vertex last = null;
        for(Vertex vertex:aSparkResult.getRoute()){
            if(last != null){
                Integer roadid = RoadData.getRoadByNodes(last.getVertexID(),vertex.getVertexID());
                boolean direction = true; //正向
                if(roadid==null){
                    roadid = RoadData.getRoadByNodes(vertex.getVertexID(),last.getVertexID());
                    direction = false;
                }
                Road road = RoadData.getRoadByID(roadid);
//                if(road == null && direction){ // 这里应该是写反了 !direction->direction
//                    roadid = RoadData.getRoadByNodes(vertex.getVertexID(),last.getVertexID());
//                    road = RoadData.getRoadByID(roadid);
//                }
                if(road != null){
                    List<Line> segments = new LinkedList<>(RoadData.getLinesByRoad(roadid));
                    result.add(new DriveSegment(roadid,road.getDistance(),road.getSpeed()*1000/3600,segments,direction));
                }
            }
            last = vertex;
        }
        addDriveSegmentWithShapePoints(end,p2.getScale(),result,false);
        return result;
    }
    private static void addDriveSegmentWithShapePoints(Point node,double scale,List<DriveSegment> result,boolean isStart){
        Road road = RoadData.getRoadByID(node.getRoadID());
        double startLength = road.getStart() == node.getNodeID()?road.getDistance()*scale:road.getDistance()*(1-scale);
        Pair<Boolean,List<Line>> lines = isStart?getStartShapePoint(node.getLineID(),node.getRoadID(),node.getNodeID()):
                getStopShapePoint(node.getLineID(),node.getRoadID(),node.getNodeID());
        result.add(new DriveSegment(road.getRoadid(),startLength,road.getSpeed()*1000/3600,lines.getB(),lines.getA()));
    }
    private static Pair<Boolean,List<Line>> getStartShapePoint(int lineID, int roadID, int nodeID){
        Road road = RoadData.getRoadByID(roadID);
        List<Line> lines = RoadData.getLinesByRoad(roadID);
        List<Line> result;
        int index = -1;
        for(Line line:lines){
            index++;
            if(line.getLineid() == lineID)
                break;
        }
        if(road.getStart() == nodeID){
            result = new LinkedList<>(lines.subList(0, index + 1));
//            Collections.reverse(result);
        }else{
            result = new LinkedList<>(lines.subList(index,lines.size()));
        }
        return new Pair<>(road.getStart() != nodeID,result);

    }
    private static Pair<Boolean,List<Line>> getStopShapePoint(int lineID, int roadID, int nodeID){
        Road road = RoadData.getRoadByID(roadID);
        List<Line> lines = RoadData.getLinesByRoad(roadID);
        List<Line> result;
        int index = -1;
        for(Line line:lines){
            index++;
            if(line.getLineid() == lineID)
                break;
        }
        if(road.getStop() == nodeID){
            result = new LinkedList<>(lines.subList(index,lines.size()));
//            Collections.reverse(result);
        }else{
            result = new LinkedList<>(lines.subList(0, index + 1));
        }
        return new Pair<>(road.getStop() != nodeID, result);
    }
    //时空角度分析
    public static double STDAnalysis(CandidatePoint p1, CandidatePoint p2,
                                          double st_mu,double st_sigma,double d_mu,double d_sigma) throws Exception {
        //spatial temporal
        double st = spatialTemporalAnalysis(p1,p2,st_mu,st_sigma);
        // direction analysis
//        double theta = Math.atan2(p2.getLine().getEnd().getY()-p2.getLine().getStart().getY(),p2.getLine().getEnd().getX()-p2.getLine().getStart().getX());
//        theta=theta<0?theta+Math.PI:theta;
//        double delta = Math.abs(theta-Math.toRadians(p2.getSamplingPoint().getAngle()));
        double fb = directionAnalysis(p2,d_mu,d_sigma);
        return st==Double.NEGATIVE_INFINITY || fb==Double.NEGATIVE_INFINITY?Double.NEGATIVE_INFINITY:
                st*fb;
    }
    public static Pair<Double, Boolean> STDRAnalysis(CandidatePoint p1, CandidatePoint p2,
                                     double st_mu,double st_sigma,double d_mu,double d_sigma) throws Exception {
        //spatial temporal
        Pair<Double, Boolean> st = spatialTemporalAnalysis2(p1,p2,st_mu,st_sigma);
        // direction analysis
        double fb = directionAnalysis(p2,d_mu,d_sigma);// *directionAnalysis(p1,p2,d_mu,d_sigma);//20200714添加匹配前后两点角度差惩罚因子
        double result =  st.getA()==Double.NEGATIVE_INFINITY || fb==Double.NEGATIVE_INFINITY?Double.NEGATIVE_INFINITY:
                st.getA()*fb;
        // 20200701 r值惩罚因子 r范围[-0.1,1.1]
//        double r = p2.getR()<=1 && p2.getR()>=0?1:1-Math.min(Math.abs(p2.getR()),Math.abs(p2.getR()-1))*2;
        double r = p2.getR()<=1 && p2.getR()>=0?1:0;// 上面写的和想的不太一样 不过效果不错 这里改一下 备份一下
        return new Pair<>(result == Double.NEGATIVE_INFINITY?Double.NEGATIVE_INFINITY:result*r,st.getB());
    }
    // 20201112 尝试距离标准化
    public static Pair<Double, Double> STDRAnalysis2(CandidatePoint p1, CandidatePoint p2,
                                                     double st_mu,double st_sigma,double d_mu,double d_sigma) throws Exception {
        //spatial temporal
        Pair<Double, Double> st = spatialTemporalAnalysis3(p1,p2,st_mu,st_sigma);
        // direction analysis
        double fb = directionAnalysis(p2,d_mu,d_sigma);
        double result =  st.getA()==Double.NEGATIVE_INFINITY || fb==Double.NEGATIVE_INFINITY?Double.NEGATIVE_INFINITY:
                st.getA()*fb;
        // 20200701 r值惩罚因子 r范围[-0.1,1.1]
//        double r = p2.getR()<=1 && p2.getR()>=0?1:1-Math.min(Math.abs(p2.getR()),Math.abs(p2.getR()-1))*2;
        double r = p2.getR()<=1 && p2.getR()>=0?1:0;// 上面写的和想的不太一样 不过效果不错 这里改一下 备份一下
//        double r = p2.getR()<=1 && p2.getR()>=0?1: // 20201231
//                Math.exp(-Math.min(Math.abs(p2.getR()),Math.abs(p2.getR()-1)));
        return new Pair<>(result == Double.NEGATIVE_INFINITY?Double.NEGATIVE_INFINITY:result*r,st.getB());
    }
    // 20201112 尝试距离标准化 并返回道路信息
    public static Triple<Double, Double, List<DriveSegment>> STDRAnalysis2WithSegment(CandidatePoint p1, CandidatePoint p2,
                                                                                      double st_mu, double st_sigma, double d_mu, double d_sigma) throws Exception {
        //spatial temporal
        Triple<Double, Double, List<DriveSegment>> st = spatialTemporalAnalysis3WithSegment(p1,p2,st_mu,st_sigma);
        // direction analysis
        double fb = directionAnalysis(p2,d_mu,d_sigma);
        double result =  st.getA()==Double.NEGATIVE_INFINITY || fb==Double.NEGATIVE_INFINITY?Double.NEGATIVE_INFINITY:
                st.getA()*fb;
        // 20200701 r值惩罚因子 r范围[-0.1,1.1]
//        double r = p2.getR()<=1 && p2.getR()>=0?1:1-Math.min(Math.abs(p2.getR()),Math.abs(p2.getR()-1))*2;
        double r = p2.getR()<=1.1&& p2.getR()>=-0.1?1:0;// 上面写的和想的不太一样 不过效果不错 这里改一下 备份一下
//        double r = p2.getR()<=1 && p2.getR()>=0?1: // 20201231
//                Math.exp(-Math.min(Math.abs(p2.getR()),Math.abs(p2.getR()-1)));
        return new Triple<>(result == Double.NEGATIVE_INFINITY?Double.NEGATIVE_INFINITY:result*r,st.getB(),st.getC());
    }

    public static double directionAnalysis(CandidatePoint point,double d_mu,double d_sigma){
        double delta = Math.toRadians(getAngleDeviation(point.getSamplingPoint(),point.getLine()));
        return gaussianFunction(delta, d_mu, d_sigma);
//        return Math.exp(-Math.pow(delta-d_mu,2)/(2*Math.pow(d_sigma,2)))/(Math.sqrt(2*Math.PI)*d_sigma);
    }
}
