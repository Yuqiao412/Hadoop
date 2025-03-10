package com.example.hadoopdemo.mapmatching.mapreduce.tod;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ODResultMap extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub
        try{
            String[] values = value.toString().split("[,]");
            context.write(new Text(values[0]),  value);
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
