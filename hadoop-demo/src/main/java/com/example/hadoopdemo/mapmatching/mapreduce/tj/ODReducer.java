package com.example.hadoopdemo.mapmatching.mapreduce.tj;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ODReducer extends Reducer<Text, Text, NullWritable, Text> {
    // hadoop多路径输出的对象
    private MultipleOutputs<NullWritable,Text> mos;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> cnt = new HashMap<>();
        for(Text val : values) {
            cnt.put(val.toString(), cnt.getOrDefault(val.toString(), 0) + 1);
        }
        for (String val : cnt.keySet()) {
            mos.write("ods", NullWritable.get(), new Text(val+","+cnt.get(val)),key.toString());
        }
    }

    @Override
    protected void setup(Context context) {
        mos=new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
