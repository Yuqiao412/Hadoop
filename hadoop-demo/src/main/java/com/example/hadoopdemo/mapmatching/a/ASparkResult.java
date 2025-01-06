package com.example.hadoopdemo.mapmatching.a;

import java.util.List;

public class ASparkResult<T> {
	private double distance;
	private List<T> route;
	
	
	public ASparkResult(double distance, List<T> route) {
		super();
		this.distance = distance;
		this.route = route;
	}


	public double getDistance() {
		return distance;
	}
	public void setDistance(double distance) {
		this.distance = distance;
	}
	public List<T> getRoute() {
		return route;
	}
	public void setRoute(List<T> route) {
		this.route = route;
	}
	
	
}
