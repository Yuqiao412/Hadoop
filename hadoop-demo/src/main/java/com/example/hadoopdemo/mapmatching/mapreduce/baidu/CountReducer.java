package com.example.hadoopdemo.mapmatching.mapreduce.baidu;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class CountReducer extends Reducer<Text, Text, NullWritable, Text> {
    // hadoop多路径输出的对象
    private MultipleOutputs<NullWritable,Text> mos;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 这里可以进一步按照小时去统计，暂时默认用天来统计
        Map<String, int[]> count = new HashMap<>();
        for(Text v : values){
            String[] contents = v.toString().split("[,]");
            Calendar time = Calendar.getInstance();
            time.setTimeInMillis(Long.parseLong(contents[1]));
            int year = time.get(Calendar.YEAR);
            int month = time.get(Calendar.MONTH)+1;
            int day = time.get(Calendar.DAY_OF_MONTH);
            String ymd = year + String.format("%02d", month) + String.format("%02d", day);
            int status = Integer.parseInt(contents[2]);
            count.putIfAbsent(ymd, new int[]{0, 0});
            int[] cnt = count.get(ymd);
            cnt[status] += 1;
        }
        for(Map.Entry<String, int[]> entry: count.entrySet()){
            mos.write("result", NullWritable.get(),
                    key.toString()+","+entry.getValue()[0]+","+entry.getValue()[1],
                    entry.getKey()+"/result");
        }
    }
    @Override
    protected void setup(Context context) {
        try {
            mos=new MultipleOutputs<>(context);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
