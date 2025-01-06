package com.example.hadoopdemo.mapmatching.a;

import com.example.hadoopdemo.mapmatching.bean.*;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.match.Common;
import com.example.hadoopdemo.mapmatching.match.PointMatching;
import com.example.hadoopdemo.mapmatching.utils.PathCommon;
import com.example.hadoopdemo.mapmatching.utils.Transform;

import java.util.*;

public class Graph {
	protected Map<Integer,Vertex> vertexs;
	private Set<Integer> openSet;
	private Set<Integer> closeSet;
//	private List<Vertex> open;
	private PriorityQueue<Vertex> open;
	private List<Vertex> close;
	private Vertex stop;
	private Vertex start;
	private Point nearPoint;//测试
	private boolean isNeedI = false;//20200302
	private boolean finish = false;
	private static final double nearDistance = 75;//邻近点容忍的最短距离，超过了这个最短路径直接干掉 0.0005对应66.3m换算方式有点问题
	private static final double nearAngle = 45;//角度小于一定阈值认为点和线的方向相同
	private boolean isShortestTime = false; //20210112 对于没有道路等级和速度的数据 取消最短时间

	
	public Graph(){
		vertexs=new HashMap<>();
		openSet = new HashSet<>();
		closeSet = new HashSet<>();
//		open = new LinkedList<>();
		open = new PriorityQueue<>(Comparator.comparingDouble(o -> o.getG() + o.getH()));
		close = new LinkedList<>();
	}

	public boolean isShortestTime() {
		return isShortestTime;
	}

	public void setShortestTime(boolean shortestTime) {
		isShortestTime = shortestTime;
	}

	public void reInit(){
		open.clear();
		close.clear();
		openSet.clear();
		closeSet.clear();
		finish=false;
		nearPoint = null;
		isNeedI = false;
		start=null;
		stop=null;
		for(Vertex v:vertexs.values()){
			v.setG(0);
			v.setH(0);
			v.setI(Double.MAX_VALUE);
			v.setPath(null);
			v.setAnchor(null);
		}
	}

	public boolean isFinish() {
		return finish;
	}
	
	public void setFinish(boolean finish) {
		this.finish = finish;
	}


	public void setNeedI(boolean needI) {
		isNeedI = needI;
	}

//	public List<Vertex> getOpen() {
//		return open;
//	}
 	public PriorityQueue<Vertex> getOpen() {
		return open;
	}

	public List<Vertex> getClose() {
		return close;
	}

	public void addToOpen(Vertex v){
		open.add(v);
		openSet.add(v.getVertexID());
	}
	private void removeFromOpen(Vertex v){
		open.remove(v);
		openSet.remove(v.getVertexID());
	}
	public void addToClose(Vertex v){
		removeFromOpen(v);
		close.add(v);
		closeSet.add(v.getVertexID());
	}
	public boolean isOpenContains(int id){
		return openSet.contains(id);
	}
	public boolean isCloseContains(int id){
		return closeSet.contains(id);
	}

//	private void sortOpenByF(){
//		open.sort(Comparator.comparingDouble(o -> o.getG() + o.getH()));
//	}
	public Vertex getVertexFromOpen(){
		if(open==null || open.size()==0){
			finish=true;
			return null;
		}
//		sortOpenByF();
//		return open.get(0);
		return open.peek();
	}
	public void setStop(int stop){
		this.stop = vertexs.get(stop);
	}
	public void setStart(int start){
		this.start = vertexs.get(start);
	}
	public Vertex getStart(){
		return this.start;
	}
	public void setNearPoint(Point p){//测试
		this.nearPoint = p;
	}
	public void update(Vertex vertex){
		if(stop==null){
			finish = true;
			return;
		}
		if(vertex.equals(stop)){
			finish = true;
			return;
		}else{
			//20200301 把close拿到了函数里面
			addToClose(vertex);
		}
		List<Triple<Vertex, Double, Integer>> adjacency = vertex.getAdjacency();
		for(Triple<Vertex, Double, Integer> a:adjacency){
//			按照逻辑这里应该是可以直接修改的
			Vertex v = a.getA();
			if(closeSet.contains(v.getVertexID()))
				continue;
			// 20201216 还是得使用r来判断是否拓展某个点
			if(this.nearPoint!=null && isNeedI){
				double r = Common.CalculateR(this.nearPoint,vertex.getPoint(),v.getPoint());
				if(r<=1.2 && r>=-0.2){
					float lineAngle = Common.CalculateAngle(vertex.getPoint(),v.getPoint());
					double angleDiff = Common.CalculateAngleDeviationByDirection(2,this.nearPoint.getAngle(),lineAngle);
					Point pedal = PointMatching.CalculatePoint(nearPoint,vertex.getPoint(),v.getPoint(), r);
					double height = Transform.getEuclideanDistance(pedal, nearPoint);
					if(angleDiff<nearAngle && height>nearDistance){
						// 这个点可能需要直接加入close表
						// 再给一次机会判断一下segment
						Integer road = RoadData.getRoadByNodes(vertex.getVertexID(),v.getVertexID());
						if(road==null)
							road = RoadData.getRoadByNodes(v.getVertexID(),vertex.getVertexID());
						List<Line> lines = RoadData.getLinesByRoad(road);
						boolean nearEnough = false;
						for(Line line : lines){
							if(!nearEnough){
								Point nearPointInSegment = Common.getSegmentNearPoint(nearPoint, line.getStart(), line.getEnd());
								nearEnough = Transform.getEuclideanDistance(nearPointInSegment, nearPoint) < nearDistance;
							}else{
								break;
							}
						}
						if(!nearEnough){
							// 20210225 屏蔽以下代码，不能直接将下一个节点完全干掉，因为节点v还可能有其他关联路，能够满足条件的，这样就一巴掌拍死了
//							addToClose(v);
							continue;
						}
					}
				}
			}

			double g = isShortestTime?vertex.getG()+a.getB()/ PathCommon.getSpeedByPathClass(a.getC()) :vertex.getG()+a.getB();
			if(!openSet.contains(v.getVertexID())){
				v.setG(g);
				v.setPath(vertex);
				v.setH(calculateH(v));
//				v.setAnchor(v.getI() == vertex.getI()?vertex.getAnchor():new IntegerPair(vertex.getVertexID(),v.getVertexID()));
				addToOpen(v);
				//20200305
			}else if(g<v.getG()){//(nearPoint==null || v.getI()<nearDistance)? g<v.getG(): i<v.getI()
				v.setG(isShortestTime?vertex.getG()+a.getB()/ PathCommon.getSpeedByPathClass(a.getC()) :vertex.getG()+a.getB());
				v.setPath(vertex);
				v.setH(calculateH(v));
//				v.setAnchor(v.getI() == vertex.getI()?vertex.getAnchor():new IntegerPair(vertex.getVertexID(),v.getVertexID()));
			}
		}
	}
	public Pair<Vertex, Double> update(Vertex vertex, Graph other, double minScore){
		if(stop==null){
			finish = true;
			return null;
		}
		if(vertex.equals(stop)){
			finish = true;
			return new Pair<>(vertex,0d);
		}
		List<Triple<Vertex, Double, Integer>> adjacency = vertex.getAdjacency();
		Vertex midPoint = null;
		for(Triple<Vertex, Double, Integer> a:adjacency){
			Vertex v = a.getA();
			if(closeSet.contains(v.getVertexID()))
				continue;
			if(!openSet.contains(v.getVertexID()) || vertex.getG()+a.getB()<v.getG()){
				v.setG(vertex.getG()+a.getB());
				v.setPath(vertex);
				v.setH(calculateH(v));
//				v.setAnchor(v.getI() == vertex.getI()?vertex.getAnchor():new IntegerPair(vertex.getVertexID(),v.getVertexID()));
			}
			if(other.isCloseContains(v.getVertexID())){
				Vertex mid = other.getVertexByID(v.getVertexID());
				if (minScore > v.getG() + mid.getG()) {
					minScore = v.getG() + mid.getG();
					midPoint = v;
				}
				addToClose(v);
			}
			else if(!openSet.contains(v.getVertexID()))
				addToOpen(v);
		}
		return midPoint!=null?new Pair<>(midPoint,minScore):null;
	}
	private double calculateH(Vertex v){
		if(isShortestTime)
			return Transform.getEuclideanDistance(v.getPoint(), stop.getPoint())/ PathCommon.getSpeedByPathClass(3);
		return Transform.getEuclideanDistance(v.getPoint(), stop.getPoint());
	}
	public Map<Integer, Vertex> getVertexs() {
		return vertexs;
	}
	public void setVertexs(Map<Integer, Vertex> vertexs) {
		this.vertexs = vertexs;
	}
	public void addVertex(Vertex vertex){
		vertexs.put(vertex.getVertexID(),vertex);
	}
	public Vertex getVertexByID(int id){
		return vertexs.get(id);
	}
}
