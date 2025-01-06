package com.example.hadoopdemo.mapmatching.bean;

public class Triple<A,B,C> extends Pair<A, B> {
	protected C c;
	
	public Triple(A a, B b, C c) {
		super(a,b);
		this.c = c;
	}
	
	public C getC() {
		return c;
	}
	public void setC(C c) {
		this.c = c;
	}
}
