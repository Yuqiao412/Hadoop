package com.example.hadoopdemo.mapmatching.bean;

public class Point {
	private int nodeID;
	private double x;
	private double y;
	private float angle;
	private int lineID;
	private int roadID;
	
	//status 通过-1状态来识别是否赋值
	private int status = -1;
//	private int time;
	private long time;
	private double velocity;
	
	
	public Point(double x, double y) {
		super();
		this.x = x;
		this.y = y;
	}

	public Point(double x, double y, float angle) {
		this.x = x;
		this.y = y;
		this.angle = angle;
	}

	public Point(Node node) {
		this.x = node.getX();
		this.y = node.getY();
		this.nodeID = node.getNodeid();
	}
	
	public Point(double x, double y, float angle, long time, double velocity, int status) {
		this.x = x;
		this.y = y;
		this.angle = angle;
		this.time = time;
		this.velocity = velocity;
		this.status = status;
	}

	public Point() {
	}


	public int getNodeID() {
		return nodeID;
	}

	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}

	public double getX() {
		return x;
	}
	public void setX(double x) {
		this.x = x;
	}
	public double getY() {
		return y;
	}
	public void setY(double y) {
		this.y = y;
	}
	public float getAngle() {
		return angle;
	}
	public void setAngle(float angle) {
		this.angle = angle;
	}

	public int getLineID() {
		return lineID;
	}

	public void setLineID(int lineID) {
		this.lineID = lineID;
	}

	public int getRoadID() {
		return roadID;
	}

	public void setRoadID(int roadID) {
		this.roadID = roadID;
	}

//	public int getTime() {
//		return time;
//	}
//
//	public void setTime(int time) {
//		this.time = time;
//	}


	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public double getVelocity() {
		return velocity;
	}

	public void setVelocity(double velocity) {
		this.velocity = velocity;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}
}
