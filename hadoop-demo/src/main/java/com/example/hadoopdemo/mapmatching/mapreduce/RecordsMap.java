package com.example.hadoopdemo.mapmatching.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class RecordsMap extends Mapper<LongWritable, Text, MyWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub
        try{
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            String[] values = value.toString().split("[,;]");
            String vehicleID = values[3];
            // 处理od时 杭州数据 20171210的文件里面 还有16年9月的数据 17年11月的数据等
            // 所以用最后一个字段判断出租车日期不合适
            // 在处理数据时 要根据前面两个字段的时间进行限制
            String timeStr = values[0]+values[1];

            Calendar time = Calendar.getInstance();
            time.setTimeInMillis(format.parse(timeStr).getTime());
            if(time.get(Calendar.YEAR)==2017 &&
                    time.get(Calendar.MONTH) == Calendar.DECEMBER &&
                    time.get(Calendar.DAY_OF_MONTH)>=4 &&
                    time.get(Calendar.DAY_OF_MONTH)<=10)
                context.write(new MyWritable(vehicleID, time.getTimeInMillis()), value);
//            context.write(new MyWritable(vehicleID, format.parse(timeStr).getTime()), value);
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
