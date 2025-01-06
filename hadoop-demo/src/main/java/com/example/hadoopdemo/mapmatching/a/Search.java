package com.example.hadoopdemo.mapmatching.a;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.jdbc.CRUD;
import com.example.hadoopdemo.mapmatching.utils.PathCommon;

import java.util.List;
import java.util.Map;

/*A*全局*/
public class Search {
	private static Graph graph;
	public static ASparkResult<Vertex> searchNodeAndSolve(int vStartID, int vEndID, long time) throws Exception {
		return searchNodeAndSolve(vStartID, vEndID, null, time,false);
	}
	public static ASparkResult<Vertex> searchNodeAndSolve(Vertex vStart, Vertex vEnd, Point nearPoint, long time) throws Exception {
		return searchNodeAndSolve(vStart, vEnd, nearPoint, time,false);
	}
	public static ASparkResult<Vertex> searchNodeAndSolve(Vertex vStart,Vertex vEnd,Point nearPoint, long time,boolean isNeedI) throws Exception {
		return searchNodeAndSolve(vStart.getVertexID(), vEnd.getVertexID(), nearPoint, time, isNeedI);
	}
	public static ASparkResult<Vertex> searchNodeAndSolve(int vStartID, int vEndID, Point nearPoint, long time, boolean isNeedI) throws Exception {
		initGraph();
		graph.reInit();
		ASpark aSpark=new ASpark(graph);
		aSpark.setStart(vStartID);
		aSpark.setStop(vEndID);
		aSpark.setNearPoint(nearPoint);
		aSpark.setTime(time);
		aSpark.setNeedI(isNeedI);
		aSpark.solvePath();
		ASparkResult<Vertex> result = null;
		List<Vertex> r= aSpark.getVertexResult();
		if(r!=null){
			result = new ASparkResult<>(aSpark.getDistance(),r);
		}
		return result;
	}
	public static void initGraph() throws Exception {
		if(graph == null){
			graph=new Graph();
			String sql="select roadid, start, stop, direction, distance, pathclass, ST_AsGeoJSON(a.point) as startpoint, ST_AsGeoJSON(b.point) as stoppoint "
					+ "from roads, nodes a, nodes b "
					+ "where start = a.nodeid and stop = b.nodeid";//start 方向是不是有问题
			List<Map<String, Object>> nodes = CRUD.read(sql, null);
			PathCommon.addNoteToGraph(graph,nodes);
		}
	}


	public static Graph getGraph() {
		return graph;
	}

	public static void setGraph(Graph graph) {
		Search.graph = graph;
	}
}
