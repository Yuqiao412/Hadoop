package com.example.hadoopdemo.mapmatching.mapreduce.tj;

import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordsMap extends Mapper<LongWritable, Text, MyWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub
        try{
            String[] values = value.toString().split(",");
            String vehicleID = values[0].trim();
            String timeStr = values[4];
            context.write(new MyWritable(vehicleID, Long.parseLong(timeStr)), value);
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
