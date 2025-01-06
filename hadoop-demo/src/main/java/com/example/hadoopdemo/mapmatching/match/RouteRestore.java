package com.example.hadoopdemo.mapmatching.match;

import com.example.hadoopdemo.mapmatching.a.ASparkResult;
import com.example.hadoopdemo.mapmatching.a.Search;
import com.example.hadoopdemo.mapmatching.a.Vertex;
import com.example.hadoopdemo.mapmatching.bean.*;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.utils.PathCommon;
import com.example.hadoopdemo.mapmatching.utils.Simplify;
import com.example.hadoopdemo.mapmatching.utils.Transform;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class RouteRestore {
	// 仅匹配不还原形状点，所有点包括抽稀后的也匹配
	public static TrajectoryResult OnlyMatching(List<Point> points) throws Exception {
		List<Integer> turningPointIndex = Simplify.getTurningPointIndex(points);
		TrajectoryResult result = new TrajectoryResult();
		if(points.size() == 0)
			return result;
		int i = 0;
		Point sure = null;
		while(sure == null && i<turningPointIndex.size()) {
			sure =  PointMatching.Matching(points.get(turningPointIndex.get(i)),true);
			i++;
		}
		if(sure ==null)
			return result;
		result.getPoints().add(sure);
		List<Point> need = new LinkedList<>();
		int lastNeedNum = -1;
		double accumulativeDistance = 0;
		for(;i<turningPointIndex.size();i++){
			Point unKnown = points.get(turningPointIndex.get(i));
			need.addAll(points.subList(turningPointIndex.get(i-1)+1, turningPointIndex.get(i)));
			if(i==turningPointIndex.size()-1){
				Point finalPoint = PointMatching.Matching(unKnown,true);
				if(finalPoint==null)
					continue;
				TrajectoryResult sureList = MakeSurePoint2(sure,need,finalPoint,true);
				if(sureList!=null)
					result.addAll(sureList);
				result.getPoints().add(finalPoint);
			}else{

				Point guessPoint = points.get(turningPointIndex.get(i+1));
				//20200312 来个打断函数
				assert sure != null;
				long timegap = PathCommon.getSecondGap(sure.getTime(), guessPoint.getTime());
				if(timegap!=0 && Transform.getEuclideanDistance(sure,guessPoint)/ timegap>(float)120*1000/3600){
					do{
						sure =  PointMatching.Matching(points.get(turningPointIndex.get(i)),true);
					}while(sure==null && ++i<turningPointIndex.size());
					result.getPoints().add(sure);
					continue;
				}
				//继续原逻辑
				need.add(unKnown);
				accumulativeDistance+=Transform.getEuclideanDistance(unKnown,guessPoint);
				boolean returnMax = accumulativeDistance>2500;
				Point guess = PointMatching.Matching(guessPoint,returnMax);
				if(guess == null){
					continue;
				}
				lastNeedNum = need.size();
				TrajectoryResult sureList = MakeSurePoint2(sure,need,guess,false);
				if(need.size()!=lastNeedNum && sureList!=null){
					result.addAll(sureList);
					sure = sureList.getPoints().get(sureList.getPoints().size()-1);
					accumulativeDistance = Transform.getEuclideanDistance(sure,guessPoint);// 即当前标志匹配点匹配后与假设点之间的距离
					need.clear();
				}

			}
		}
		return result;
	}
	// 抽稀后的点 不再进行地图匹配
	public static TrajectoryResult Restore(List<Point> points) throws Exception {
		List<Point> pList = Simplify.getTurningPoint(points);
		TrajectoryResult result = new TrajectoryResult();
		if(pList.size() == 0)
			return result;
		int i = 0;
		Point sure = null;
		while(sure == null && i<pList.size()) {
			sure = PointMatching.Matching(pList.get(i), true);
			i++;
		}
		if(sure ==null)
			return result;
		result.getPoints().add(sure);
		List<Point> need = new LinkedList<>();
		int lastNeedNum;
		double accumulativeDistance = 0;
		for(;i<pList.size();i++){
			Point unKnown = pList.get(i);
			if(i==pList.size()-1){
				Point finalPoint = PointMatching.Matching(unKnown,true);
				if(finalPoint==null)
					continue;
				TrajectoryResult sureList = MakeSurePoint(sure,need,finalPoint,true);
				if(sureList!=null)
					result.addAll(sureList);
				result.getPoints().add(finalPoint);
			}else{
				need.add(unKnown);
				Point guessPoint = pList.get(i+1);
//				accumulativeDistance+= Transform.getEuclideanDistance(sure,guessPoint);
				accumulativeDistance+=Transform.getEuclideanDistance(unKnown,guessPoint);
				boolean returnMax = accumulativeDistance>2500;
				Point guess = PointMatching.Matching(guessPoint,returnMax);
				if(guess == null){
					continue;
				}
				lastNeedNum = need.size();
				TrajectoryResult sureList = MakeSurePoint(sure,need,guess,false);
				if(need.size()!=lastNeedNum && sureList!=null){//有些可能一辈子匹配不上 所以垂点改为反向 need.size()==0改为和上一次不一样
					result.addAll(sureList);
					sure = sureList.getPoints().get(sureList.getPoints().size()-1);
//					accumulativeDistance = 0;
					accumulativeDistance = Transform.getEuclideanDistance(sure,guessPoint);// 即当前标志匹配点匹配后与假设点之间的距离
					need.clear();//这边应该能接着
					// GetPedal函数中的没有匹配上的点，如果都搞定了应该是个空数组
				}
				
			}
		}
		return result;
	}
	// 抽稀后的点在轨迹还原之后 仍匹配到还原的路径上
	public static TrajectoryResult MapMatching(List<Point> points) throws Exception {
		List<Integer> turningPointIndex = Simplify.getTurningPointIndex(points);
		TrajectoryResult result = new TrajectoryResult();
		if(points.size() == 0)
			return result;
		int i = 0;
		Point sure = null;
		while(sure == null && i<turningPointIndex.size()) {
			sure =  PointMatching.Matching(points.get(turningPointIndex.get(i)),true);
			i++;
		}
		if(sure ==null)
			return result;
		result.getPoints().add(sure);
		List<Point> need = new LinkedList<>();
		int lastNeedNum = -1;
		double accumulativeDistance = 0;
		for(;i<turningPointIndex.size();i++){
			Point unKnown = points.get(turningPointIndex.get(i));
			need.addAll(points.subList(turningPointIndex.get(i-1)+1, turningPointIndex.get(i)));
			if(i==turningPointIndex.size()-1){
				Point finalPoint = PointMatching.Matching(unKnown,true);
				if(finalPoint==null)
					continue;
				TrajectoryResult sureList = MakeSurePoint(sure,need,finalPoint,true);
				if(sureList!=null)
					result.addAll(sureList);
				result.getPoints().add(finalPoint);
			}else{

				Point guessPoint = points.get(turningPointIndex.get(i+1));
				//20200312 来个打断函数
				assert sure != null;
				if(Transform.getEuclideanDistance(sure,guessPoint)/ PathCommon.getSecondGap(sure.getTime(), guessPoint.getTime())>(float)120*1000/3600){
					do{
						sure =  PointMatching.Matching(points.get(turningPointIndex.get(i)),true);
					}while(sure==null && ++i<turningPointIndex.size());
					result.getPoints().add(sure);
					continue;
				}
				//继续原逻辑
				need.add(unKnown);
				accumulativeDistance+=Transform.getEuclideanDistance(unKnown,guessPoint);
				boolean returnMax = accumulativeDistance>2500;
				Point guess = PointMatching.Matching(guessPoint,returnMax);
				if(guess == null){
					continue;
				}
				lastNeedNum = need.size();
				TrajectoryResult sureList = MakeSurePoint(sure,need,guess,false);
				if(need.size()!=lastNeedNum && sureList!=null){
					result.addAll(sureList);
					sure = sureList.getPoints().get(sureList.getPoints().size()-1);
					accumulativeDistance = Transform.getEuclideanDistance(sure,guessPoint);// 即当前标志匹配点匹配后与假设点之间的距离
					need.clear();
				}
				
			}
		}
		return result;
	}
	public static TrajectoryResult MakeSurePoint(Point p1, List<Point> points, Point p2, boolean isFinal) throws Exception {
		Point start = GetOorD(p1, true);
		Point end = GetOorD(p2, false);
		if(start ==null || end == null )
			return null;
		if(start.getRoadID()==end.getRoadID()){
			if(!isFinal)
				return null;
			Road road = RoadData.getRoadByID(start.getRoadID());
			List<Line> lines = RoadData.getLinesByRoad(start.getRoadID());
			//这时候start和end反着呢
			if(road.getStart()==start.getNodeID()){
				Collections.reverse(lines);
			}
			return GetPedalInInvertedOrder(p1, p2, points, null, lines, null,true);
		}
		TrajectoryResult result = new TrajectoryResult();
		List<Line> startShapePoints = getStartShapePoint(start.getLineID(), start.getRoadID(), start.getNodeID());
		List<Line> stopShapePoints = getStopShapePoint(end.getLineID(), end.getRoadID(), end.getNodeID());
		if(start.getNodeID()==end.getNodeID()){
			result.addAll(GetPedalInInvertedOrder(p1, p2, points, null, startShapePoints, stopShapePoints,isFinal));
		}else{
			Vertex startNode=new Vertex(start);
			Vertex endNode=new Vertex(end);
			Point nearPoint = null;
			boolean isNeedI =false;// 是否需要最后的点到折线的最短距离计算 20200302
			if(points!=null && points.size()>0) {
				nearPoint = points.get(points.size()-1);
				double r1 = Common.CalculateR(nearPoint,p1,start);
				double r2 = Common.CalculateR(nearPoint,end,p2);
//				isNeedI = !((r1>=0 && r1<=1) || (r2>=0 && r2<=1));
				isNeedI = !((r1>=0 && r1<=1 && Transform.getEuclideanDistance(nearPoint, Common.getSegmentNearPoint(p1, start, nearPoint))<=75) ||
						(r2>=0 && r2<=1 && Transform.getEuclideanDistance(nearPoint, Common.getSegmentNearPoint(p2, nearPoint, end))<=75));
			}

			long time = PathCommon.getSecondGap(p1.getTime(), p2.getTime());
			//加上time 保证每个点的效率！！
			ASparkResult<Vertex> aSparkResult = Search.searchNodeAndSolve(startNode, endNode, nearPoint, time, isNeedI);//A*
			if(aSparkResult==null)
				return null;
			double dis = aSparkResult.getDistance();
			
			if(time!=0 && dis/time>(float)120*1000/3600)
				return null;
			result.addAll(GetPedalInInvertedOrder(p1, p2, points, aSparkResult.getRoute(), startShapePoints, stopShapePoints,isFinal));
		}
		
		return result;
		
	}
	// 不还原形状点
	public static TrajectoryResult MakeSurePoint2(Point p1, List<Point> points, Point p2, boolean isFinal) throws Exception {
		Point start = GetOorD(p1, true);
		Point end = GetOorD(p2, false);
		if(start ==null || end == null )
			return null;
		if(start.getRoadID()==end.getRoadID()){
			if(!isFinal)
				return null;
			Road road = RoadData.getRoadByID(start.getRoadID());
			List<Line> lines = RoadData.getLinesByRoad(start.getRoadID());
			//这时候start和end反着呢
			if(road.getStart()==start.getNodeID()){
				Collections.reverse(lines);
			}
			return GetPedalInInvertedOrder2(p1, p2, points, null, lines, null,true);
		}
		TrajectoryResult result = new TrajectoryResult();
		List<Line> startShapePoints = getStartShapePoint(start.getLineID(), start.getRoadID(), start.getNodeID());
		List<Line> stopShapePoints = getStopShapePoint(end.getLineID(), end.getRoadID(), end.getNodeID());
		if(start.getNodeID()==end.getNodeID()){
			result.addAll(GetPedalInInvertedOrder2(p1, p2, points, null, startShapePoints, stopShapePoints,isFinal));
		}else{
			Vertex startNode=new Vertex(start);
			Vertex endNode=new Vertex(end);
			Point nearPoint = null;
			boolean isNeedI =false;// 是否需要最后的点到折线的最短距离计算 20200302
			if(points!=null && points.size()>0) {
				nearPoint = points.get(points.size()-1);
				double r1 = Common.CalculateR(nearPoint,p1,start);
				double r2 = Common.CalculateR(nearPoint,end,p2);
//				isNeedI = !((r1>=0 && r1<=1) || (r2>=0 && r2<=1));
				isNeedI = !((r1>=0 && r1<=1 && Transform.getEuclideanDistance(nearPoint, Common.getSegmentNearPoint(p1, start, nearPoint))<=75) ||
						(r2>=0 && r2<=1 && Transform.getEuclideanDistance(nearPoint, Common.getSegmentNearPoint(p2, nearPoint, end))<=75));
			}

			long time = PathCommon.getSecondGap(p1.getTime(), p2.getTime());
			//加上time 保证每个点的效率！！
			ASparkResult<Vertex> aSparkResult = Search.searchNodeAndSolve(startNode, endNode, nearPoint, time, isNeedI);//A*
			if(aSparkResult==null)
				return null;
			double dis = aSparkResult.getDistance();

			if(time!=0 && dis/time>(float)120*1000/3600)
				return null;
			result.addAll(GetPedalInInvertedOrder2(p1, p2, points, aSparkResult.getRoute(), startShapePoints, stopShapePoints,isFinal));
		}

		return result;

	}

	private static List<Line> GetLinesByRoute(List<Vertex> route){
		if(route.size()<=1){
			return new ArrayList<>();
		}
		int i = 0;
		List<Line> lines = new LinkedList<>();
		while(i<route.size()-1){
			Point p1=route.get(i).getPoint();
			Point p2=route.get(i+1).getPoint();
			
			int start = p1.getNodeID();
			int stop = p2.getNodeID();
			boolean reverse = false;
			Integer roadid = RoadData.getRoadByNodes(start, stop);
			if(roadid==null){
				roadid = RoadData.getRoadByNodes(stop, start);
				if(roadid==null)
					continue;
				reverse = true;
			}
			List<Line> segments = new LinkedList<>(RoadData.getLinesByRoad(roadid));
			if(reverse)
				Collections.reverse(segments);
			lines.addAll(segments);
			i++;
		}
		return lines;
	}
	// TODO: 2020/1/16 倒数第一个如果有问题就会过很多点 要改一下
	private static TrajectoryResult GetPedalInInvertedOrder(Point begin, Point end, List<Point> points, List<Vertex> route,
															List<Line> start, List<Line> stop, boolean isFinal) throws Exception {
		//上个函数有的死活进不去的 就要倒着来
//		List<Point> results = new ArrayList<>();
		// 计算某个时间 某个路段的速度和车流量
		int status = begin.getStatus();
//		long timeGap = end.getTime() - begin.getTime();
//		double speedGap = end.getVelocity() - begin.getVelocity();
//		double totalDistance = 0;
		List<Line> lines = new ArrayList<>(start);
		if(route!=null)
			lines.addAll(GetLinesByRoute(route));
		if(stop!=null)
			lines.addAll(stop);
		// 计算总距离
//		for(Line line: lines){
//			totalDistance+=Transform.getEuclideanDistance(line.getStart(),line.getEnd());
//		}
//		double curDistance = 0;

		TrajectoryResult result = new TrajectoryResult();

		Collections.reverse(points);
		Collections.reverse(lines);
		int j = 0;
		int lastLineID = -1;
		boolean isPointMatch = false;// 是否有点已经匹配到路上，因为只记录确定点和当前匹配点之间的路
		Point lastShapePoint=null;
		Point shape = null;
		//记录每次循环的变量
		int lineid = -1;
		int direction = 0;
		float lineAngle = 0;
		Point p1 = null;
		Point p2 = null;
		//20200302
		Line lastLine = null;
		boolean firstLoop = true;

		List<Pair<Point,Line>> passedShapePoints = new ArrayList<>();// 2021/10/22 用于临时记录途经点 稍后再将其加入结果集 为了计算途经点的时间和速度
		Point pedal = null; // 2021/10/22 把垂点放在外面 方便计算途径点的具体时间和速度
		while(j<lines.size()){//这个条件不对
			Line line = lines.get(j);
			if(shape == null){
				lineid = line.getLineid();
				direction = line.getDirection();
				lineAngle = Common.CalculateAngle(line.getStart().getX(), line.getStart().getY(), line.getEnd().getX(), line.getEnd().getY());
				p1 = new Point(line.getStart().getX(), line.getStart().getY());
				p2 = new Point(line.getEnd().getX(), line.getEnd().getY());
				if(j==0){//从p1到前进方向上最近的路段点
					shape = Common.GetShapePointByLineDirection(p1, p2, direction, true, begin);
					p1 = shape;
					p2 = end;
				}else if(j==lines.size()-1){
					shape = Common.GetShapePointByLineDirection(p1, p2, direction, true, begin);
					p2 = shape==p1?p2:p1;
					p1 = begin;
					shape = p1;
				}else if(lastShapePoint==null)
					shape = Common.GetShapePointByLineDirection(p1, p2, direction, true, begin);
				else
					shape = Common.GetShapePointByLineDirection(p1, p2, direction, false, lastShapePoint);
				//少了一个从p1到shape 以及最后 shape到p2的两个线段
			}else if(shape==p1){
				p2 = result.getPoints().get(result.getPoints().size()-1);
			}else{
				p1 = result.getPoints().get(result.getPoints().size()-1);
			}
			// 注释以下代码 2021/10/22
//			boolean hasPointMatch = false;//当前路段有没有点可以匹配的上
			int i = 0;
			pedal = null;// 20211026
			for(;i<points.size();i++){
				Point p = points.get(i);
				assert p1 != null;
				pedal = GetPedal(p1,p2,p,line);
				if(pedal==null) {
					//20200302 再不行 改成只对靠近点采取这个操作
					if(lastLine!=null && firstLoop && i==0){
						Point endPoint = Common.GetEdgePoint(lastLine.getStart(),lastLine.getEnd(),lastLine.getDirection(),false,p,false);
						Point startPoint = Common.GetEdgePoint(line.getStart(),line.getEnd(),line.getDirection(),true,p,false);
						double r = Common.CalculateR(p,startPoint.getX(),startPoint.getY(),endPoint.getX(),endPoint.getY());
						if(r>=0 &&r<=1){
							Point tmpPedal = Common.GetEdgePoint(line.getStart(),line.getEnd(),line.getDirection(),true,p,true);
							if(Transform.getEuclideanDistance(tmpPedal,p)<50) {
								pedal = new Point(tmpPedal.getX(),tmpPedal.getY());
								pedal.setRoadID(line.getRoadid());
								pedal.setLineID(lineid);
								pedal.setAngle(p.getAngle());
								pedal.setTime(p.getTime());
								pedal.setStatus(p.getStatus());
								pedal.setVelocity(p.getVelocity());
								// 注释以下代码 2021/10/22
//								result.getPoints().add(pedal);
//								hasPointMatch = true;
								isPointMatch = true;
							}
						}
					}
				}else{
					float deviation = Common.CalculateAngleDeviationByDirection(direction, p.getAngle(), lineAngle);
					if(deviation<=40){
						// 注释以下代码 2021/10/22
//						result.getPoints().add(pedal);
//						hasPointMatch = true;
						isPointMatch = true;
					}

				}
				if(pedal!=null)
					break;

			}
			int pointSize = result.getPoints().size();// 2021/10/22
			if(pedal!=null){

				// 20211026
//				points.subList(0, i + 1).clear();
				if(i==points.size())
					points.subList(0, i).clear();
				else
					points.subList(0, i + 1).clear();
				// start 2021/10/22
				if(pointSize>0){
					addPointAndLineResult(result, passedShapePoints, pedal, result.getPoints().get(pointSize-1));
				}else if(isFinal){
					addPointAndLineResult(result, passedShapePoints, pedal, end);
				}
				result.getPoints().add(pedal);
				passedShapePoints.clear();
				// end 20211022
				lastLine = null;//20200302
				firstLoop = false;
				continue;//20200302
			}
			//2021/10/22再次注释，应该保留尽可能多的速度记录
////			 这种算法可能不太好 20211021 试试看用平均速度
//			double meanSpeed = totalDistance/timeGap;
//			long endTime = (long)((totalDistance-curDistance)/meanSpeed+begin.getTime());
////			long endTime = (long)((totalDistance-curDistance)/totalDistance*timeGap+begin.getTime());
////			double endSpeed = (totalDistance-curDistance)/totalDistance*speedGap+begin.getVelocity();
//			curDistance+=Transform.getEuclideanDistance(line.getStart(),line.getEnd());
////			long startTime =  (long)((totalDistance-curDistance)/totalDistance*timeGap+begin.getTime());
////			double startSpeed = (totalDistance-curDistance)/totalDistance*speedGap+begin.getVelocity();
//			long startTime = (long)((totalDistance-curDistance)/meanSpeed+begin.getTime());

			if((isFinal || isPointMatch) && lastLineID != lineid && j != lines.size()-1){
				//再加线 这边的results的最后一个 可能是添加的匹配点 求距离匹配点的最近最远是不科学的
				assert shape != null;
				shape.setStatus(status);
//				shape.setVelocity(startSpeed);//20211021
				// start 2021/10/22 注释
//				shape.setVelocity(meanSpeed);
//				shape.setTime(startTime);
//				result.getPoints().add(shape);
				// end 2021/10/22 注释
				passedShapePoints.add(new Pair<>(shape, line));
				lastShapePoint=shape;
				lastLineID = lineid;
			}


			lastLine = line;//20200302
			shape = null;
//			result.addLine(new LineResult(startTime,endTime,line.getLineid(),line.getRoadid(),startSpeed,endSpeed));//20211021
//			result.addLine(new LineResult(startTime,endTime,line.getLineid(),line.getRoadid(),meanSpeed,meanSpeed));//2021/10/22
			j++;
		}
		// TODO: 2021/10/23 轨迹起始点到记录的最后一个点之间的线段和点记录在这里添加 代码类似第一个todo
		int pointsSize = result.getPoints().size();
		addPointAndLineResult(result, passedShapePoints, begin, pointsSize==0?end:result.getPoints().get(pointsSize-1));
		//返回结果
		Collections.reverse(points);
		Collections.reverse(result.getPoints());
		Collections.reverse(result.getLines());
		return result;
	}

	/**
	 * 用于向结果集添加途经点，并根据前后浮动车记录计算途经点的时间和速度
	 * @param result 结果集
	 * @param passedShapePoints 途径的形状点 倒序的 但是不添加 begin 和 end
	 * @param begin 前一个浮动车记录 轨迹原始顺序
	 * @param end 后一个浮动车记录
	 */
	public static void addPointAndLineResult(TrajectoryResult result, List<Pair<Point,Line>> passedShapePoints, Point begin, Point end){
//		int type = 0; // 20221124
		double distance = 0;
		Point last = end;
		for(Pair<Point, Line> pair:passedShapePoints){
			distance+=Transform.getEuclideanDistance(last, pair.getA());
			last = pair.getA();
		}
		distance+=Transform.getEuclideanDistance(last, begin);
		last = end;
		double curDistance = 0;
		// 默认用两点之间的平均速度 或者多加几个速度 方便后续处理
		long timeGap = PathCommon.getSecondGap(begin.getTime(),end.getTime());
		double meanSpeed = distance/ timeGap;
//		// 20221124 异常检测1
//		if(meanSpeed>150){
////			type = 1;
//			System.out.println("1");
//		}
		//20220430 计算加速度
//		double a = (end.getVelocity()-begin.getVelocity())/timeGap;
		double a = (Math.pow(end.getVelocity(),2)-Math.pow(begin.getVelocity(),2))/2/distance;
		double exitSpeed = end.getVelocity();
		for(Pair<Point, Line> pair:passedShapePoints){
			curDistance+=Transform.getEuclideanDistance(last, pair.getA());
			pair.getA().setVelocity(meanSpeed);
			pair.getA().setTime((long)((distance-curDistance)/meanSpeed*1000)+begin.getTime());// 忘了*1000了 20221124
			//20220430 计算一下加速度的驶入和驶出速度
			double enterSpeed = Math.sqrt(Math.pow(begin.getVelocity(),2)+2*a*(distance-curDistance));
			// 20221124 异常 代码假定是线性加速 同时使用上面的距离、时间、首尾速度 其实是有问题的 不满足假设
//			if(Double.isNaN(enterSpeed)){
////				type |= 2;
//				System.out.println("2");
//			}
//			double deltaT = (enterSpeed-begin.getVelocity())/a;
//			double enterT = begin.getTime()+deltaT;
			result.getPoints().add(pair.getA());
			result.addLine(new LineResult(pair.getA().getTime(), last.getTime(),pair.getB().getLineid(),pair.getB().getRoadid(),meanSpeed, enterSpeed, exitSpeed));
			last = pair.getA();
			exitSpeed = enterSpeed;
		}
//		 少了last与start之间的记录 2022年4月30日
		result.addLine(new LineResult(begin.getTime(), last.getTime(), begin.getLineID(), begin.getRoadID(),meanSpeed,begin.getVelocity(), exitSpeed));
//		return type;
	}

	// 不还原形状点
	private static TrajectoryResult GetPedalInInvertedOrder2(Point begin, Point end, List<Point> points, List<Vertex> route,
															List<Line> start, List<Line> stop, boolean isFinal) throws Exception {
		List<Line> lines = new ArrayList<>(start);
		if(route!=null)
			lines.addAll(GetLinesByRoute(route));
		if(stop!=null)
			lines.addAll(stop);
		TrajectoryResult result = new TrajectoryResult();

		Collections.reverse(points);
		Collections.reverse(lines);
		int j = 0;
		int lastLineID = -1;
		boolean isPointMatch = false;// 是否有点已经匹配到路上，因为只记录确定点和当前匹配点之间的路
		Point lastShapePoint=null;
		Point shape = null;
		//记录每次循环的变量
		int lineid = -1;
		int direction = 0;
		float lineAngle = 0;
		Point p1 = null;
		Point p2 = null;
		//20200302
		Line lastLine = null;
		boolean firstLoop = true;
		while(j<lines.size()){//这个条件不对
			Line line = lines.get(j);
			if(shape == null){
				lineid = line.getLineid();
				direction = line.getDirection();
				lineAngle = Common.CalculateAngle(line.getStart().getX(), line.getStart().getY(), line.getEnd().getX(), line.getEnd().getY());
				p1 = new Point(line.getStart().getX(), line.getStart().getY());
				p2 = new Point(line.getEnd().getX(), line.getEnd().getY());
				if(j==0){//从p1到前进方向上最近的路段点
					shape = Common.GetShapePointByLineDirection(p1, p2, direction, true, begin);
					p1 = shape;
					p2 = end;
				}else if(j==lines.size()-1){
					shape = Common.GetShapePointByLineDirection(p1, p2, direction, true, begin);
					p2 = shape==p1?p2:p1;
					p1 = begin;
					shape = p1;
				}else if(lastShapePoint==null)
					shape = Common.GetShapePointByLineDirection(p1, p2, direction, true, begin);
				else
					shape = Common.GetShapePointByLineDirection(p1, p2, direction, false, lastShapePoint);
				//少了一个从p1到shape 以及最后 shape到p2的两个线段
			}else if(shape==p1){
				p2 = result.getPoints().get(result.getPoints().size()-1);
			}else{
				p1 = result.getPoints().get(result.getPoints().size()-1);
			}
			boolean hasPointMatch = false;//当前路段有没有点可以匹配的上
			int i = 0;
			for(;i<points.size();i++){
				Point p = points.get(i);
				assert p1 != null;
				Point pedal = GetPedal(p1,p2,p,line);
				if(pedal==null) {
					//20200302 再不行 改成只对靠近点采取这个操作
					if(lastLine!=null && firstLoop && i==0){
						Point endPoint = Common.GetEdgePoint(lastLine.getStart(),lastLine.getEnd(),lastLine.getDirection(),false,p,false);
						Point startPoint = Common.GetEdgePoint(line.getStart(),line.getEnd(),line.getDirection(),true,p,false);
						double r = Common.CalculateR(p,startPoint.getX(),startPoint.getY(),endPoint.getX(),endPoint.getY());
						if(r>=0 &&r<=1){
							Point tmpPedal = Common.GetEdgePoint(line.getStart(),line.getEnd(),line.getDirection(),true,p,true);
							if(Transform.getEuclideanDistance(tmpPedal,p)<50) {
								pedal = new Point(tmpPedal.getX(),tmpPedal.getY());
								pedal.setLineID(lineid);
								pedal.setRoadID(line.getRoadid());
								pedal.setAngle(p.getAngle());
								pedal.setTime(p.getTime());
								pedal.setStatus(p.getStatus());
								pedal.setVelocity(p.getVelocity());
								result.getPoints().add(pedal);
								hasPointMatch = true;
								isPointMatch = true;
							}
						}
					}
				}else{
					float deviation = Common.CalculateAngleDeviationByDirection(direction, p.getAngle(), lineAngle);
					if(deviation<=40){
						result.getPoints().add(pedal);
						hasPointMatch = true;
						isPointMatch = true;
					}

				}
				if(hasPointMatch)
					break;

			}
			if(hasPointMatch){
				points.subList(0, i + 1).clear();
				lastLine = null;//20200302
				firstLoop = false;
				continue;//20200302
			}
			if((isFinal || isPointMatch) && lastLineID != lineid && j != lines.size()-1){
				//再加线 这边的results的最后一个 可能是添加的匹配点 求距离匹配点的最近最远是不科学的
				assert shape != null;
				lastShapePoint=shape;
				lastLineID = lineid;
			}
			lastLine = line;//20200302
			shape = null;
			j++;
		}
		//返回结果
		Collections.reverse(points);
		Collections.reverse(result.getPoints());
		return result;
	}

	private static Point GetPedal(Point p1, Point p2, Point p, Line line){
		double r = Common.CalculateR(p, p1.getX(), p1.getY(), p2.getX(), p2.getY());
		if(r>0&&r<1){
			Point result;
			if(p1.getX() == p2.getX()){
				result=new Point(p1.getX(), p.getY());
			}
			else{
				double xr=p1.getX() + r * (p2.getX() - p1.getX());
				double yr=p1.getY() + r * (p2.getY() - p1.getY());
				result=new Point(xr, yr);
			}
			result.setLineID(line.getLineid());
			result.setRoadID(line.getRoadid());
			result.setAngle(p.getAngle());
			result.setTime(p.getTime());
			result.setStatus(p.getStatus());
			result.setVelocity(p.getVelocity());
			return result;
		}
		return null;
	}
	public static Point GetOorD(Point p,boolean isStart){
		//如果是起点的话,我们计算就是点到线段尾点的距离
		Line line = RoadData.getLineByID(p.getLineID());
		if(line!=null){
			int roadid = line.getRoadid();
			int direction = line.getDirection();
			Point point_min=line.getStart();
			Point point_max=line.getEnd();
			int flag;//"start":1;"stop":0
			Point target;
			switch (direction) {
			case 2://正向
				flag = isStart ? 0 : 1;
				break;
			case 3://反向
				flag = isStart ? 1 : 0;
				break;
			case 0:
			case 1:
				float lineAngle1 = Common.CalculateAngle(point_min.getX(),point_min.getY(),point_max.getX(),point_max.getY());
				float angleDeviation1 = Common.CalculateAngleDeviationWithoutConsideringTwoWay(direction, p.getAngle(), lineAngle1);
				float lineAngle2 = Common.CalculateAngle(point_max.getX(),point_max.getY(),point_min.getX(),point_min.getY());
				float angleDeviation2 = Common.CalculateAngleDeviationWithoutConsideringTwoWay(direction, p.getAngle(), lineAngle2);
				flag = angleDeviation1 < angleDeviation2 == isStart ? 0 : 1;// 和上面一个逻辑
				break;
			default:
				return null;
			}
			target = getNodeIDByRoad(roadid, flag);
			target.setLineID(p.getLineID());//存放的待匹配点的线id
			target.setRoadID(roadid);
			return target;
		}
		return null;
	}
	private static Point getNodeIDByRoad(int roadID, int flag){
		Road road =RoadData.getRoadByID(roadID);
		Node node = flag==1?RoadData.getNodeByID(road.getStart()):RoadData.getNodeByID(road.getStop());
		Point target = new Point(node.getX(),node.getY());
		target.setNodeID(node.getNodeid());
		target.setRoadID(roadID);
		return target;
	}

	private static List<Line> getStartShapePoint(int lineID, int roadID, int nodeID){
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
			Collections.reverse(result);
		}else{
			result = new LinkedList<>(lines.subList(index,lines.size()));
		}
		return result;

	}
	private static List<Line> getStopShapePoint(int lineID, int roadID, int nodeID){
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
			Collections.reverse(result);
		}else{
			result = new LinkedList<>(lines.subList(0, index + 1));
		}
		return result;
	}
}
