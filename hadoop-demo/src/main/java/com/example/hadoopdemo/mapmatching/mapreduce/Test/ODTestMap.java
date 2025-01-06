package com.example.hadoopdemo.mapmatching.mapreduce.Test;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class ODTestMap extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub
//        try{
//            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
//            String[] contents = value.toString().split("[,]");
//            String timeStr = contents[0]+contents[1];
//
//            Calendar time = Calendar.getInstance();
//            time.setTimeInMillis(format.parse(timeStr).getTime());
//            int day = time.get(Calendar.DAY_OF_MONTH);
//            if(day<3 || day>11)
//                context.write(NullWritable.get(),  value);
//        }catch (Exception e){
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }

        try{
            String[] contents = value.toString().split("\\s+");
            double lng = Double.parseDouble(contents[7]);
            double lat = Double.parseDouble(contents[6]);
            if(lng<121.988048 && lng>120.851726 && lat>30.752303 && lat<31.546376){
                context.write(NullWritable.get(), value);
            }
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
