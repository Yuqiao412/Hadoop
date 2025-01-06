package com.example.hadoopdemo.mapmatching.bean;

/**
 * Created by AJQK on 2019/12/27.
 */
public class IntegerPair extends Pair<Integer,Integer> implements Comparable<IntegerPair> {

    public IntegerPair(Integer a, Integer b) {
        super(a, b);
    }

    @Override
    public int compareTo(IntegerPair o) {
        return a.equals(o.a)?b.compareTo(o.b):a.compareTo(o.a);
    }
}
