package com.example.hadoopdemo.mapmatching.mapreduce.tod;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ODResultReducer extends Reducer<Text, Text, NullWritable, Text> {
    public static int dayCount = 2;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int[][] count = new int[2][24];
        for(Text value: values){
            String[] content = value.toString().split("[,]");
            int weight = Integer.parseInt(content[2]);
            int hour = Integer.parseInt(content[1]);
            count[weight][hour]++;
        }
        StringBuilder sb = new StringBuilder(String.valueOf(key));
        for(int i=0;i<2;i++){
            for(int j=0;j<24;j++){
                // 统计的几天 就除以几
                sb.append(",").append((float)count[i][j]/dayCount);
            }
        }
        context.write(NullWritable.get(), new Text(sb.toString()));
    }
}
