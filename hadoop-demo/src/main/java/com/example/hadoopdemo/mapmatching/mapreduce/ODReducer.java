package com.example.hadoopdemo.mapmatching.mapreduce;

import com.example.hadoopdemo.mapmatching.a.Search;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ODReducer extends Reducer<MyWritable, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable,Text> mos;
    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context) {
        try {
            Integer last = null;
            for (Text val : values){
                String[] contents = val.toString().split("[,;]");
//                int weight=Integer.parseInt(contents[8])==1?1:0;
                int weight=Integer.parseInt(contents[8]);
                if(last!=null && last!=weight){
//                    context.write(NullWritable.get(), new Text(val)); // 正常输出
                    mos.write("od", NullWritable.get(), new Text(val), contents[0]); // 按日期输出
                }
                last = weight;
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    @Override
    protected void setup(Reducer<MyWritable, Text, NullWritable, Text>.Context context) {
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
