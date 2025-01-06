package com.example.hadoopdemo.mapmatching.mapreduce.congestion.freeflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Calendar;
import java.util.PriorityQueue;

public class FlowCountReducer extends Reducer<LongWritable, Text, NullWritable,Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int[] count = new int[24];
        int total = 0;
        for(Text val:values){
            String[] contents = val.toString().split("[,]");

            Calendar startTime = Calendar.getInstance();
            startTime.setTimeInMillis(Long.parseLong(contents[2]));
            int enterHour = startTime.get(Calendar.HOUR_OF_DAY);

            Calendar endTime = Calendar.getInstance();
            endTime.setTimeInMillis(Long.parseLong(contents[3]));
            int exitHour = endTime.get(Calendar.HOUR_OF_DAY);
            count[enterHour]++;
            if(enterHour != exitHour){
                count[exitHour]++;
            }
            total++;
        }
        StringBuilder sb = new StringBuilder(String.valueOf(key.get()));
        sb.append(",").append(total);
        for(int c:count){
            sb.append(",").append(c);
        }
        context.write(NullWritable.get(), new Text(sb.toString()));

    }
}
