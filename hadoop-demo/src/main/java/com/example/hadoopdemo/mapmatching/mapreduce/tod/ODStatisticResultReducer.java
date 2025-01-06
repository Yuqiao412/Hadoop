package com.example.hadoopdemo.mapmatching.mapreduce.tod;

import com.example.hadoopdemo.mapmatching.bean.Triple;
import com.example.hadoopdemo.utils.HdfsUtils;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ODStatisticResultReducer extends Reducer<Text, Text, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Triple<Integer, Double, Long>> count = new HashMap<>();//数量，长度，距离
        Set<Integer> daySet = new HashSet<>();
        for(Text value: values){
            String[] content = value.toString().split("[,]");
            int weight = Integer.parseInt(content[3]);
            int hour = Integer.parseInt(content[2]);
            int day = Integer.parseInt(content[1]);
            daySet.add(day);
            String k = getKey(day, hour, weight);
            Triple<Integer, Double, Long> data;
            if(count.containsKey(k)){
                data = count.get(k);
                data.setA(data.getA()+1);
                data.setB(data.getB()+Double.parseDouble(content[4]));
                data.setC(data.getC()+Long.parseLong(content[5]));
            }else{
                data = new Triple<>(1, Double.parseDouble(content[4]), Long.parseLong(content[5]));
                count.put(k, data);
            }
        }
        for(Integer d : daySet){
            for(int i=0;i<2;i++){
                for(int j=0;j<24;j++){
                    Triple<Integer, Double, Long> data = count.get(getKey(d,j,i));
                    if(data == null)
                        continue;
                    // 小区id, 日期，小时，空重状态，距离（米），时间（秒）
                    context.write(NullWritable.get(),new Text(key.toString()+","+d+","+j+","+i+","+data.getA()+","+data.getB()+","+data.getC())); // 按日期输出
                }
            }

        }
    }
    private String getKey(int day, int hour, int weight){
        return day+","+hour+","+weight;
    }
}
