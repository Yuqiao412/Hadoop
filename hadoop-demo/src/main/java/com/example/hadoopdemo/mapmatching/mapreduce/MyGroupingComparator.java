package com.example.hadoopdemo.mapmatching.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupingComparator extends WritableComparator {
    protected MyGroupingComparator() {
        super(MyWritable.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MyWritable aBean = (MyWritable)a;
        MyWritable bBean = (MyWritable)b;
        return aBean.getVehicleID().compareTo(bBean.getVehicleID());
    }

}
