package com.example.hadoopdemo.mapmatching.mapreduce.parallel;

import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.SimpleDateFormat;
import java.util.Arrays;

public class ODGroupMap extends Mapper<LongWritable, Text, MyWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub
        try{
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            String[] contents = value.toString().split("[;]");
            String[] values = contents[0].split("[,]");
            String vehicleID = values[2];
            String timeStr = values[0]+values[1];
            long time = format.parse(timeStr).getTime();
            // time, x, y;候选点1;候选点2
            context.write(new MyWritable(vehicleID, time), new Text(time+","+values[3]+","+values[4]+";"+String.join(";", Arrays.copyOfRange(contents, 1, values.length))));
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
