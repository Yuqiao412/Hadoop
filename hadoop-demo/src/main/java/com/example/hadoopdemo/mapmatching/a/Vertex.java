package com.example.hadoopdemo.mapmatching.a;


import com.example.hadoopdemo.mapmatching.bean.IntegerPair;
import com.example.hadoopdemo.mapmatching.bean.Pair;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.bean.Triple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Vertex {
	private int vertexID;
	private Point point;
	private double G;
	private double H;
	//20200303
	private Double I = Double.MAX_VALUE;//测试 计算与接近点的距离
	private IntegerPair anchor;
	private List<Triple<Vertex, Double, Integer>> adjacency;
	private Set<Integer> adjacencySet;
	private Vertex path;//父节点
	
	public Vertex(Point point){
		this.vertexID = point.getNodeID();
		this.point = point;
		adjacency = new ArrayList<>();
		adjacencySet = new HashSet<>();
		path = null;
	}

	public int getVertexID() {
		return vertexID;
	}
	public void setVertexID(int vertexID) {
		this.vertexID = vertexID;
	}
	public Point getPoint() {
		return point;
	}
	public void setPoint(Point point) {
		this.point = point;
		if(point.getNodeID()!=vertexID)
			vertexID = point.getNodeID();
	}
	public double getG() {
		return G;
	}
	public void setG(double g) {
		G = g;
	}
	public double getH() {
		return H;
	}
	public void setH(double h) {
		H = h;
	}
	
	public double getI() {
		return I;
	}

	public void setI(double i) {
		I = i;
	}


	public List<Triple<Vertex, Double, Integer>> getAdjacency() {
		return adjacency;
	}

	public void setAdjacency(List<Triple<Vertex, Double, Integer>> adjacency) {
		this.adjacency = adjacency;
	}

	public void addAdjacency(Triple<Vertex, Double, Integer> p){
		this.adjacency.add(p);
		this.adjacencySet.add(p.getA().getVertexID());
	}
	public void addAdjacencyWithCheck(Triple<Vertex, Double, Integer> p){
		if(!this.adjacencySet.contains(p.getA().getVertexID())) {
			this.adjacency.add(p);
			this.adjacencySet.add(p.getA().getVertexID());
		}
	}


	public Vertex getPath() {
		return path;
	}
	public void setPath(Vertex path) {
		this.path = path;
	}

	public IntegerPair getAnchor() {
		return anchor;
	}

	public void setAnchor(IntegerPair anchor) {
		this.anchor = anchor;
	}
	
}
