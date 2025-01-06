package com.example.hadoopdemo.mapmatching.a;

import com.example.hadoopdemo.mapmatching.bean.IntegerPair;
import com.example.hadoopdemo.mapmatching.bean.Pair;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.bean.Triple;
import com.example.hadoopdemo.mapmatching.match.Common;
import com.example.hadoopdemo.mapmatching.utils.PathCommon;
import com.example.hadoopdemo.mapmatching.utils.Transform;

import java.util.*;

public class GraphBak {
	protected Map<Integer,Vertex> vertexs;
	private Set<Integer> openSet;
	private Set<Integer> closeSet;
	private List<Vertex> open;
	private List<Vertex> close;
	private Vertex stop;
	private Vertex start;
	private Point nearPoint;//测试
	private boolean isNeedI = false;//20200302
	private boolean finish = false;
	private static final double nearDistance = 75;//邻近点容忍的最短距离，超过了这个最短路径直接干掉 0.0005对应66.3m换算方式有点问题
	private boolean isShortestTime = true;
	private Set<IntegerPair> obstacles = new TreeSet<>();
	public static int repeatCount = 0;


	public GraphBak(){
		vertexs=new HashMap<>();
		openSet = new HashSet<>();
		closeSet = new HashSet<>();
		open = new LinkedList<>();
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
		repeatCount = 0;
		finish=false;
		nearPoint = null;
		isNeedI = false;
		obstacles = new TreeSet<>();
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

	public List<Vertex> getOpen() {
		return open;
	}

	public List<Vertex> getClose() {
		return close;
	}

	
//	public void modifyOpen(List<Vertex> points){
//		final Map<Integer, Vertex> map = points.stream().collect(
//	            Collectors.toMap(Vertex::getVertexID, (p) -> p));
//		open.removeIf(t->!map.containsKey(t.getVertexID()));
//	}

//	public Map<Integer, Vertex> getVertexs() {
//		return vertexs;
//	}
//	public void setVertexs(Map<Integer, Vertex> vertexs) {
//		this.vertexs = vertexs;
//	}
//	public void addVertex(Vertex vertex){
//		vertexs.put(vertex.getVertexID(),vertex);
//	}
//	public Vertex getVertexByID(int id){
//		return vertexs.get(id);
//	}
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

	private void sortOpenByF(){
		open.sort(Comparator.comparingDouble(o -> o.getG() + o.getH()));
	}
	public Vertex getVertexFromOpen(){
		if(open==null || open.size()==0){
			finish=true;
			return null;
		}
		sortOpenByF();
		return open.get(0);
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
//			totalCount++;
			//20200301 加了个判断 接近点如果在最短路径的端点之外就不需要判断
			if(!isNeedI || vertex.getI()<=nearDistance){
//			if(!isNeedI || (vertex.getI().getA()<=nearDistance && vertex.getI().getB()<nearAngle)){
				finish = true;
			}else if(repeatCount>1){
				finish = true;
				vertex.setPath(null);
				return;
			}else{
				repeatCount++;
				obstacles.add(vertex.getAnchor());
				// 重置一些属性
				open.clear();
				close.clear();
				openSet.clear();
				closeSet.clear();
				finish=false;
				for(Vertex v:vertexs.values()){
					v.setG(0);
					v.setH(0);
					v.setI(Double.MAX_VALUE);
					v.setPath(null);
					v.setAnchor(null);
				}
				addToOpen(start);
			}
//			finish = true;
			return;
		}else{
			//20200301 把close拿到了函数里面
			addToClose(vertex);
		}
		List<Triple<Vertex, Double, Integer>> adjacency = vertex.getAdjacency();
		for(Triple<Vertex, Double, Integer> a:adjacency){
//			按照逻辑这里应该是可以直接修改的
			Vertex v = a.getA();
			if(closeSet.contains(v.getVertexID()) || obstacles.contains(new IntegerPair(vertex.getVertexID(),v.getVertexID())))
				continue;
			//20200305
			double i = calculateI(vertex, v, vertex.getI());
			double g = isShortestTime?vertex.getG()+a.getB()/ PathCommon.getSpeedByPathClass(a.getC()) :vertex.getG()+a.getB();
			if(!openSet.contains(v.getVertexID())){
				v.setG(g);
				v.setPath(vertex);
				v.setH(calculateH(v));
				v.setI(i);//20200301 新增一个计算函数
				v.setAnchor(v.getI() == vertex.getI()?vertex.getAnchor():new IntegerPair(vertex.getVertexID(),v.getVertexID()));
//				//20200303
//				if(v.getI()==-1)
//					addToClose(v);
//				else
//					addToOpen(v);
				addToOpen(v);
				//20200305
			}else if(g<v.getG()){//(nearPoint==null || v.getI()<nearDistance)? g<v.getG(): i<v.getI()
				v.setG(isShortestTime?vertex.getG()+a.getB()/ PathCommon.getSpeedByPathClass(a.getC()) :vertex.getG()+a.getB());
				v.setPath(vertex);
				v.setH(calculateH(v));
				v.setI(i);//20200301 新增一个计算函数
				v.setAnchor(v.getI() == vertex.getI()?vertex.getAnchor():new IntegerPair(vertex.getVertexID(),v.getVertexID()));
//				//20200303
//				if(v.getI()==-1)
//					addToClose(v);
			}
		}
	}
	public Pair<Vertex, Double> update(Vertex vertex, GraphBak other, double minScore){
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
				v.setI(calculateI(vertex, v, vertex.getI()));
				v.setAnchor(v.getI() == vertex.getI()?vertex.getAnchor():new IntegerPair(vertex.getVertexID(),v.getVertexID()));
			}
			if(other.isCloseContains(v.getVertexID())){
				Vertex mid = other.getVertexByID(v.getVertexID());
				if (minScore > v.getG() + mid.getG()) {
					minScore = v.getG() + mid.getG();
					midPoint = v;
				}
				addToClose(v);
			}
//			//20200303
//			else if(v.getI()==-1)
//				addToClose(v);
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
	
	//仅仅是排除有问题的路段，对于高架拐外路而言，不会排除，能够保证准确性和效率
	private double calculateI(Vertex v1, Vertex v2, double lastI){

		if(nearPoint==null || lastI == 0)
			return 0;
//		Integer road = RoadData.getRoadByNodes(v1.getVertexID(),v2.getVertexID());
//		if(road==null)
//			road = RoadData.getRoadByNodes(v2.getVertexID(),v1.getVertexID());
//		List<Line> lines = RoadData.getLinesByRoad(road);
//		double d = Double.MAX_VALUE;
//		for(Line line:lines){
//
//		}
		double r = Common.CalculateR(nearPoint, v1.getPoint(),
				v2.getPoint());
		Point p;
		if(r>0 && r<1){
			p = Common.getSegmentNearPoint(v1.getPoint(), v2.getPoint(), nearPoint);
		}else if(r<=0){
			p = v1.getPoint();
		}else {
			p = v2.getPoint();
		}
		return Math.min(Transform.getEuclideanDistance(p, nearPoint), lastI);
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
