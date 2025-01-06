package com.example.hadoopdemo.mapmatching.HMM;


import com.example.hadoopdemo.mapmatching.HMM.bean.CandidateItem;
import com.example.hadoopdemo.mapmatching.HMM.bean.CandidatePoint;
import com.example.hadoopdemo.mapmatching.HMM.bean.DriveSegment;
import com.example.hadoopdemo.mapmatching.bean.*;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.match.RouteRestore;

import java.util.*;

public class Matching {
    private static final double envelope = 0.00075;//搜索半径 大约150m

    private static final double mu = 0;// 计算观测概率的期望
    private static final double fSigma = 50;// 计算观测概率的标准差 浮动车数据算法改为50 以减少距离的比重

    private static final double directionalMu = 0;
    private static final double fDirectionalSigma = Math.toRadians(30); // 浮动车数据算法改为30 以减少角度的比重

    private static final int k = 10;// 最大参与点数
    private static final int dl = 8;
    // 针对浮动车数据试试结果
    public static List<Point> doMatch(List<Point> points) throws Exception {
        List<CandidateItem> last = new ArrayList<>();
        CandidateItem maxScore = null;//只得到最后一次的最大值 虽然下面每次都刷新
        CandidateItem lastMaxScore = null;//记录上一次循环的最高分点，如果当前没有解算出来，需要打断则添加上一个点的结果
        List<Point> result = new LinkedList<>();//最终结果
        for(Point point: points) {
            lastMaxScore = maxScore;
            maxScore = null;
            List<Line> lines = RoadData.getLinesByExtent(point.getX() - envelope, point.getX() + envelope, point.getY() - envelope, point.getY() + envelope);
//            if(lines.size()==0)
//                lines = RoadData.getLinesByExtent(new Rectangle(point.getX() - 2*envelope, point.getX() + 2*envelope, point.getY() - 2*envelope, point.getY() + 2*envelope));
            Map<Integer, CandidatePoint> tmpCandidatePoints = new HashMap<>();
            Map<Integer, CandidatePoint> other = new HashMap<>();
            // 尝试一条路只保留一个点 20210323
//            Map<Integer, List<CandidatePoint>> tmpCandidatePoints = new HashMap<>();
//            Map<Integer, List<CandidatePoint>> other = new HashMap<>();
//            int totalSize = 0;
//            int tmpSize = 0;

            for(Line line:lines){
                Pair<Float,CandidatePoint> pedal = CommonMethod.CalculatePointWithAllParams(point, line);
                if(pedal.getA()>60)//角度差大于60的直接pass snail代码中取60
                    continue;
                //20200710加上角度限制
                int key = pedal.getB().getLine().getRoadid();
                //20210323 增加【0,1】的选项
                if(pedal.getB().getR()>=0 && pedal.getB().getR()<=1){
                    tmpCandidatePoints.put(key, pedal.getB());
                }
                else if(pedal.getB().getHeight()<30 || (pedal.getB().getR()>=-0.1 && pedal.getB().getR()<=1.1)) {
                    //默认范围[0,1] 浮动个小范围 不然有些线没办法选中
                    //20210323
//                    if(!tmpCandidatePoints.containsKey(key)){
//                        tmpCandidatePoints.put(key, new ArrayList<>());
//                    }
//                    tmpCandidatePoints.get(key).add(pedal.getB());
//                    totalSize++;
                    if(!tmpCandidatePoints.containsKey(key) || tmpCandidatePoints.get(key).getHeight()>pedal.getB().getHeight()){
                        tmpCandidatePoints.put(key, pedal.getB());
                    }
                }
                else{
                    other.put(key, pedal.getB());
                }

            }
            if(tmpCandidatePoints.size()==0){
                tmpCandidatePoints.putAll(other);
            }
            List<CandidatePoint> candidatePoints = new ArrayList<>(tmpCandidatePoints.values());
            // 尽量保证不同的路都有机会
            // 20210323
            if(tmpCandidatePoints.size()>k){
                // 20200707 发射概率加上方向信息 //20210323 直接恢复
                candidatePoints.sort((o1, o2) -> Double.compare(getInitializationProbability(o2,false),
                        getInitializationProbability(o1,false)));
                candidatePoints = new ArrayList<>(candidatePoints.subList(0,k));
            }
            // 就差计算那个std了
            // tmp 记录当前循环中找到的待匹配点
            List<CandidateItem> tmp = new ArrayList<>(candidatePoints.size());
            if(last.size() == 0){
                maxScore = calculateOP2(candidatePoints,last);
                continue;
            }else{
                Map<Integer, List<CandidateItem>> scoreAndDistance = new HashMap<>();
                double maxDistance = Double.MIN_VALUE;
                double minDistance = Double.MAX_VALUE;
                boolean isNeedMaxMinDistance = false; // 20220726
                for (CandidateItem candidateItem : last) {
                    if(candidateItem.getScore()==Double.NEGATIVE_INFINITY)
                        continue;
                    int repeatStartOrEndCount = 0;
                    for (int j = 0; j < candidatePoints.size(); j++) {
                        // 20201112 尝试距离归一化
                        Pair<Double,Double> m = CommonMethod.STDRAnalysis2(candidateItem.getPoint(), candidatePoints.get(j), mu, fSigma, directionalMu, fDirectionalSigma);
                        if(m.getA()==Double.NEGATIVE_INFINITY)
                            continue;
                        maxDistance = Math.max(m.getB(), maxDistance);
                        minDistance = Math.min(m.getB(), minDistance);
                        CandidateItem item = new CandidateItem(candidatePoints.get(j));
                        item.setScore(m.getA());
                        item.setDistance(m.getB());// 20220728
                        item.setLast(candidateItem);
                        List<CandidateItem> candidateList = scoreAndDistance.getOrDefault(j, null);
                        if(candidateList==null){
                            candidateList = new ArrayList<>();
                            scoreAndDistance.put(j,candidateList);
                        } else{
                            isNeedMaxMinDistance = true; // 20220726
                        }
                        repeatStartOrEndCount++;
                        candidateList.add(item);
                    }
                    isNeedMaxMinDistance = isNeedMaxMinDistance || repeatStartOrEndCount>1;
                }
                for(List<CandidateItem> candidateList:scoreAndDistance.values()){
                    CandidateItem curMaxScore = null;
                    for(CandidateItem item: candidateList){
                        double norDistance = !isNeedMaxMinDistance || Math.abs(maxDistance-minDistance)<dl?1:
                                (maxDistance-0.5*minDistance-0.5*item.getDistance())/(maxDistance-minDistance);
                        item.setScore(item.getScore()*norDistance + item.getLast().getScore());
                        // 20220728 以下两个if的第二个判断条件,和第三个条件的前半部分为新增
                        if(curMaxScore==null
                                || (Math.abs(curMaxScore.getScore()-curMaxScore.getLast().getScore()-item.getScore()+item.getLast().getScore())<=1E-6
                                && Math.abs(maxDistance-minDistance)>=dl
                                && item.getDistance()<curMaxScore.getDistance())
                                || (Math.abs(curMaxScore.getScore()-item.getScore())<=5E-4
                                && item.getDistance()<curMaxScore.getDistance())
                                || curMaxScore.getScore()<item.getScore()){
                            curMaxScore = item;
                        }
                        if(maxScore==null
                                || (Math.abs(maxScore.getScore()-maxScore.getLast().getScore()-item.getScore()+item.getLast().getScore())<=1E-6
                                && Math.abs(maxDistance-minDistance)>=dl
                                && item.getDistance()<maxScore.getDistance())
                                || (Math.abs(maxScore.getScore()-item.getScore())<=5E-4
                                && item.getDistance()<maxScore.getDistance())
                                || item.getScore()>maxScore.getScore()){
                            maxScore = item;
                        }
                    }
                    tmp.add(curMaxScore);
                }
            }
            if(tmp.size()==0 || maxScore == null) {
                //新逻辑 没找到就打断
                addData(lastMaxScore,result);
                //重新计算发射概率（观测概率)
                last.clear();
                maxScore = calculateOP2(candidatePoints,last);
            }else{
                last = new ArrayList<>(tmp);
            }

        }
        addData(maxScore==null?lastMaxScore:maxScore,result);
        return result;
    }
    // 针对浮动车数据来个轨迹还原
    public static TrajectoryResult restoration(List<Point> points) throws Exception {
        List<CandidateItem> last = new ArrayList<>();
        CandidateItem maxScore = null;//只得到最后一次的最大值 虽然下面每次都刷新
        CandidateItem lastMaxScore = null;//记录上一次循环的最高分点，如果当前没有解算出来，需要打断则添加上一个点的结果
        TrajectoryResult result = new TrajectoryResult();//最终结果
//        int i=0;
        for(Point point: points) {
//            if((++i)==5){
//                System.out.println("pause");
//            }
            lastMaxScore = maxScore;
            maxScore = null;
            List<Line> lines = RoadData.getLinesByExtent(point.getX() - envelope, point.getX() + envelope, point.getY() - envelope, point.getY() + envelope);
//            Map<Integer, List<CandidatePoint>> tmpCandidatePoints = new HashMap<>();
//            Map<Integer, List<CandidatePoint>> other = new HashMap<>();
//            int totalSize = 0;
//            int tmpSize = 0;
            Map<Integer, CandidatePoint> tmpCandidatePoints = new HashMap<>();
            Map<Integer, CandidatePoint> other = new HashMap<>();

            for(Line line:lines){
                Pair<Float,CandidatePoint> pedal = CommonMethod.CalculatePointWithAllParams(point, line);
                if(pedal.getA()>60)//角度差大于60的直接pass snail代码中取60
                    continue;
                //20200710加上角度限制
                int key = pedal.getB().getLine().getRoadid();
                if(pedal.getB().getR()>=0 && pedal.getB().getR()<=1){
                    tmpCandidatePoints.put(key, pedal.getB());
                }
                else if(pedal.getB().getHeight()<30 || (pedal.getB().getR()>=-0.1 && pedal.getB().getR()<=1.1)) {
                    //默认范围[0,1] 浮动个小范围 不然有些线没办法选中
//                    if(!tmpCandidatePoints.containsKey(key)){
//                        tmpCandidatePoints.put(key, new ArrayList<>());
//                    }
//                    tmpCandidatePoints.get(key).add(pedal.getB());
//                    totalSize++;
                    if(!tmpCandidatePoints.containsKey(key) || tmpCandidatePoints.get(key).getHeight()>pedal.getB().getHeight()){
                        tmpCandidatePoints.put(key, pedal.getB());
                    }
                }
                else{
//                    if(!other.containsKey(key)){
//                        other.put(key, new ArrayList<>());
//                    }
//                    other.get(key).add(pedal.getB());
//                    tmpSize++;
                    if(!other.containsKey(key) || other.get(key).getHeight()>pedal.getB().getHeight()){
                        other.put(key, pedal.getB());
                    }

                }

            }
//            if(totalSize==0) {
//                tmpCandidatePoints.putAll(other);
//                totalSize+=tmpSize;
//            }
//            List<CandidatePoint> candidatePoints = new ArrayList<>();
            if(tmpCandidatePoints.size()==0){
                tmpCandidatePoints.putAll(other);
            }
            List<CandidatePoint> candidatePoints = new ArrayList<>(tmpCandidatePoints.values());
            // 尽量保证不同的路都有机会

//            if(totalSize>k){
            if(tmpCandidatePoints.size()>k){
//                int index = 0;
//                while(candidatePoints.size()<k){
//                    List<CandidatePoint> tmp = new ArrayList<>();
//                    for(Integer key:tmpCandidatePoints.keySet()){
//                        List<CandidatePoint> groupPoints = tmpCandidatePoints.get(key);
//                        // 第一次先给每条路上的所有候选点排序
//                        if(index==0){
//                            groupPoints.sort((o1, o2) -> Double.compare(getInitializationProbability(o2,false),
//                                    getInitializationProbability(o1,false)));
//                        }
//                        if(index<groupPoints.size()){
//                            tmp.add(groupPoints.get(index));
//                        }
//                    }
//                    if(tmp.size()<=k-candidatePoints.size()){
//                        candidatePoints.addAll(tmp);
//                    }else{
//                        tmp.sort((o1, o2) -> Double.compare(getInitializationProbability(o2,false),
//                                getInitializationProbability(o1,false)));
//                        candidatePoints.addAll(tmp.subList(0, k-candidatePoints.size()));
//                    }
//
//                    index++;
//                }
                candidatePoints.sort((o1, o2) -> Double.compare(getInitializationProbability(o2,false),
                        getInitializationProbability(o1,false)));
                candidatePoints = new ArrayList<>(candidatePoints.subList(0,k));
            }
//            else{
//                for(List<CandidatePoint> values:tmpCandidatePoints.values())
//                    candidatePoints.addAll(values);
//            }
            List<CandidateItem> tmp = new ArrayList<>(candidatePoints.size());
            if(last.size() == 0){
                maxScore = calculateOP2(candidatePoints,last);
                continue;
            }else{
                Map<Integer, List<CandidateItem>> scoreAndDistance = new HashMap<>();
                double maxDistance = Double.MIN_VALUE;
                double minDistance = Double.MAX_VALUE;
                boolean isNeedMaxMinDistance = false;
                for (CandidateItem candidateItem : last) {
                    if(candidateItem.getScore()==Double.NEGATIVE_INFINITY)
                        continue;
                    int repeatStartOrEndCount = 0;
                    for (int j = 0; j < candidatePoints.size(); j++) {
//                        if(candidateItem.getPoint().getLine().getLineid() == 866896 && candidatePoints.get(j).getLine().getLineid() == 913135)
//                            System.out.println(1);
//                        if(candidateItem.getPoint().getLine().getLineid() == 910226 && candidatePoints.get(j).getLine().getLineid() == 910231)
//                            System.out.println(2);
                        // 20201112 尝试距离归一化
                        Triple<Double,Double,List<DriveSegment>> m = CommonMethod.STDRAnalysis2WithSegment(candidateItem.getPoint(), candidatePoints.get(j), mu, fSigma, directionalMu, fDirectionalSigma);
                        if(m.getA()==Double.NEGATIVE_INFINITY)
                            continue;
                        maxDistance = Math.max(m.getB(), maxDistance);
                        minDistance = Math.min(m.getB(), minDistance);
                        CandidateItem item = new CandidateItem(candidatePoints.get(j));
                        item.setScore(m.getA());
                        item.setLast(candidateItem);
                        item.setDistance(m.getB());// 20220728
                        item.setLines(m.getC());
                        List<CandidateItem> candidateList = scoreAndDistance.getOrDefault(j, null);
                        if(candidateList==null){
                            candidateList = new ArrayList<>();
                            scoreAndDistance.put(j,candidateList);
                        }else{
                            isNeedMaxMinDistance = true;
                        }
                        repeatStartOrEndCount++;
                        candidateList.add(item);
                    }
                    isNeedMaxMinDistance = isNeedMaxMinDistance || repeatStartOrEndCount>1;
                }
                for(List<CandidateItem> candidateList:scoreAndDistance.values()){
                    CandidateItem curMaxScore = null;
                    for(CandidateItem item: candidateList){
                        double norDistance = !isNeedMaxMinDistance || Math.abs(maxDistance-minDistance)<dl?1:
                                (maxDistance-0.5*minDistance-0.5*item.getDistance())/(maxDistance-minDistance);
                        item.setScore(item.getScore()*norDistance + item.getLast().getScore());
                        // 20220728 以下两个if的第二个判断条件,和第三个条件的前半部分为新增
                        if(curMaxScore==null
                                || (Math.abs(curMaxScore.getScore()-curMaxScore.getLast().getScore()-item.getScore()+item.getLast().getScore())<=1E-6
                                && Math.abs(maxDistance-minDistance)>=dl
                                && item.getDistance()<curMaxScore.getDistance())
                                || (Math.abs(curMaxScore.getScore()-item.getScore())<=5E-4
                                && item.getDistance()<curMaxScore.getDistance())
                                || curMaxScore.getScore()<item.getScore()){
                            curMaxScore = item;
                        }
                        if(maxScore==null
                                || (Math.abs(maxScore.getScore()-maxScore.getLast().getScore()-item.getScore()+item.getLast().getScore())<=1E-6
                                && Math.abs(maxDistance-minDistance)>=dl
                                && item.getDistance()<maxScore.getDistance())
                                || (Math.abs(maxScore.getScore()-item.getScore())<=5E-4
                                && item.getDistance()<maxScore.getDistance())
                                || item.getScore()>maxScore.getScore()){
                            maxScore = item;
                        }
                    }
                    tmp.add(curMaxScore);
                }
            }
            if(tmp.size()==0 || maxScore == null) {
                //新逻辑 没找到就打断
                addDataWithSegment(lastMaxScore,result);
//                int type = addDataWithSegment(lastMaxScore,result);
//                result.updateErrorType(type);
//                if(result.getErrorType() == 3){
//                    return result;
//                }
                //重新计算发射概率（观测概率)
                last.clear();
                maxScore = calculateOP2(candidatePoints,last);
            }else{
                last = new ArrayList<>(tmp);
            }

        }
        addDataWithSegment(maxScore==null?lastMaxScore:maxScore,result);
//        int type = addDataWithSegment(maxScore==null?lastMaxScore:maxScore,result);
//        result.updateErrorType(type);
        return result;
    }
    // 添加R值
    private static CandidateItem calculateOP2(List<CandidatePoint> candidatePoints,List<CandidateItem> result){
        CandidateItem maxScore = null;
        for (CandidatePoint candidatePoint : candidatePoints) {
            CandidateItem item = new CandidateItem(candidatePoint);
            //20200707 初始状态也考虑方向角度
            double score = getInitializationProbability(item.getPoint(),true);
//            double r = item.getPoint().getR()<=1 && item.getPoint().getR()>=0?1:0;
//            double score = CommonMethod.observationProbability(candidatePoint.getHeight(),mu,sigma)*
//                    CommonMethod.directionAnalysis(candidatePoint,directionalMu,directionalSigma)*
//                    r;
            if(score==Double.NEGATIVE_INFINITY){
                continue;
            }
            if(maxScore==null || score>maxScore.getScore()){
                maxScore = item;
            }
            item.setScore(score);
            result.add(item);
        }
        return maxScore;
    }
    private static double getInitializationProbability(CandidatePoint point,boolean isR){
        // 20201231 原来最后是0
//        double r = !isR || (point.getR()<=1.1 && point.getR()>=-0.1)?1:
//                Math.exp(-Math.min(Math.abs(point.getR()),Math.abs(point.getR()-1)));
        double r = !isR || (point.getR()<=1.1 && point.getR()>=-0.1)?1:0;
        return CommonMethod.gaussianFunction(point.getHeight(),mu,fSigma)*
                CommonMethod.directionAnalysis(point,directionalMu,fDirectionalSigma)*
                r;
    }
    private static void addData(CandidateItem maxScore, List<Point> result){
        List<Point> tmp = new LinkedList<>();
        if(maxScore==null)
            return;
        do{
            tmp.add(maxScore.getPoint().getCandidatePoint());
        } while((maxScore = maxScore.getLast())!=null);
        Collections.reverse(tmp);
        result.addAll(tmp);
    }
    private static void addDataWithSegment(CandidateItem maxScore, TrajectoryResult result){
        if(maxScore==null)
            return;
//        int type = 0;// 20221124
        TrajectoryResult curResult = new TrajectoryResult();
        List<Pair<Point,Line>> passedShapePoints = new ArrayList<>();
//        List<Point> tmpPoints = new LinkedList<>();
//        List<LineResult> tmpLines = new LinkedList<>();
        Point last = null;
        do{
            Point point = maxScore.getPoint().getCandidatePoint();
            // 复用之前的方法 其中passedShapePoints是倒序的 start 和 end也是反的
            if(last!=null){
                RouteRestore.addPointAndLineResult(curResult, passedShapePoints, point, last);
//                int t = RouteRestore.addPointAndLineResult(curResult, passedShapePoints, point, last);
//                type |= t;
//                if(type==3){
//                    return 3;
//                }
            }
            curResult.getPoints().add(point);
            passedShapePoints.clear();
//            tmpPoints.add(point);
            List<DriveSegment> segments = maxScore.getLines();
            if(segments==null)
                continue;
            for(int i=segments.size()-1;i>=0;i--){
                List<Line> lines = segments.get(i).getLines();
                boolean direction = segments.get(i).isDirection();
                // 如果是正的 反而需要倒着来
                if(direction){
                    for(int j=lines.size()-1;j>=0;j--){
                        Line line = lines.get(j);
//                        tmpPoints.add(line.getStart());
                        Point p = line.getStart();
                        p.setStatus(point.getStatus());
                        passedShapePoints.add(new Pair<>(p, line));
                    }
                }else{
                    for (Line line : lines) {
//                        tmpPoints.add(line.getEnd());
                        Point p = line.getEnd();
                        p.setStatus(point.getStatus());
                        passedShapePoints.add(new Pair<>(p, line));
                    }
                }
            }
            passedShapePoints.remove(passedShapePoints.size()-1);
            last = point;
        } while((maxScore = maxScore.getLast())!=null);
        //
//        Collections.reverse(tmpPoints);
//        Collections.reverse(tmpLines);
        Collections.reverse(curResult.getPoints());
        Collections.reverse(curResult.getLines());
        result.addAll(curResult);
//        return type;
    }
}
