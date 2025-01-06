package com.example.hadoopdemo.mapmatching.mapreduce.baidu;

import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.SimpleDateFormat;

public class RecordsMap extends Mapper<LongWritable, Text, MyWritable, Text> {
    private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub
        try{
            String[] values = value.toString().split("[,]");
            String vehicleID = values[3];
            String timeStr = values[0]+values[1];
            context.write(new MyWritable(vehicleID, format.parse(timeStr).getTime()), value);
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
