package com.example.hadoopdemo.mapmatching.bean;


import com.example.hadoopdemo.mapmatching.RTree.Rectangle;

/**
 * Created by AJQK on 2020/1/3.
 */
public interface Geometry {
    Rectangle getExtent();
    boolean intersect(Rectangle rectangle);
}
