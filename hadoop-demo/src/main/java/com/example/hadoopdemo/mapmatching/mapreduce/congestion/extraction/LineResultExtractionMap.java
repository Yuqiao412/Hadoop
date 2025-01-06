package com.example.hadoopdemo.mapmatching.mapreduce.congestion.extraction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineResultExtractionMap extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub
        try{
            String[] values = value.toString().split("[,]");
            long roadID = Long.parseLong(values[1]);
            if(roadID == 111 || roadID == 64 || roadID == 87)
                context.write(NullWritable.get(),  value);
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
