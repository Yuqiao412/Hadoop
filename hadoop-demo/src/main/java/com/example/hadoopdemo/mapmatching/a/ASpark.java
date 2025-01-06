package com.example.hadoopdemo.mapmatching.a;

import com.example.hadoopdemo.mapmatching.bean.Point;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ASpark {
	private Graph graph;
	private int start;
	private int stop;
	private long time;
	
	private Point nearPoint;//测试
	private boolean isNeedI = false;

	
	public ASpark(Graph graph) {
		super();
		this.graph = graph;
	}

	public Graph getGraph() {
		return graph;
	}

	public void setGraph(Graph graph) {
		this.graph = graph;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getStop() {
		return stop;
	}

	public void setStop(int stop) {
		this.stop = stop;
	}
	
	public Point getNearPoint() {
		return nearPoint;
	}

	public void setNearPoint(Point nearPoint) {
		this.nearPoint = nearPoint;
	}

	public boolean isNeedI() {
		return isNeedI;
	}

	public void setNeedI(boolean needI) {
		isNeedI = needI;
	}

	public void solvePath(){
		graph.setStop(stop);
		graph.setNearPoint(nearPoint);
		graph.setStart(start);
		graph.setNeedI(isNeedI);
		graph.addToOpen(graph.getStart());
		Vertex current = graph.getVertexFromOpen();
		while(!graph.isFinish()){
			if(time != 0 && (graph.isShortestTime()?
					current.getG()>time:
					current.getG()/time>(float)120*1000/3600)){
				graph.setFinish(true);
				break;
			}
			graph.update(current);
//			graph.addToClose(current);//放到update里面//20200301
			current = graph.getVertexFromOpen();
		}
	}
	
	public List<Integer> getResult(){
		List<Integer> result = new LinkedList<>();
		if(!graph.isFinish())
			solvePath();
		result.add(stop);
		Vertex current = graph.getVertexByID(stop);
		do{
			Vertex last = current.getPath();
			if(last == null){
				return null;
			}
			result.add(last.getVertexID());
			current=last;
		}while(current.getVertexID()!=start);
		Collections.reverse(result);
		return result;
	}
	public List<Vertex> getVertexResult(){
		List<Vertex> result = new LinkedList<>();
		if(!graph.isFinish())
			solvePath();
		Vertex current = graph.getVertexByID(stop);
		if(current==null)
			return null;
		result.add(current);
		do{
			Vertex last = current.getPath();
			if(last == null){
				return null;
			}
			result.add(last);
			current=last;
		}while(current.getVertexID()!=start);
		Collections.reverse(result);
		return result;
	}
	public double getDistance(){
		return graph.getVertexByID(stop).getG();
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}
	
	
}
