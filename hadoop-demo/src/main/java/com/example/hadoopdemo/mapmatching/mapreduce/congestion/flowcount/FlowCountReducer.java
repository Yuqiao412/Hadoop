package com.example.hadoopdemo.mapmatching.mapreduce.congestion.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.PriorityQueue;

public class FlowCountReducer extends Reducer<LongWritable, Text, NullWritable,Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int cnt = 0;
        for(Text val:values){
            cnt++;
        }
        context.write(NullWritable.get(), new Text(key+","+cnt));
    }
}
