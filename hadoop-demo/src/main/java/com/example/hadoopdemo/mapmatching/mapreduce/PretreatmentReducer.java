package com.example.hadoopdemo.mapmatching.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.List;
// 如果提取上下车点 用check 如果做拥堵识别用check2
public class PretreatmentReducer extends Reducer<MyWritable, Text, NullWritable, Text> {
    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context) {
        try {
            List<List<VehiclePoint>> points = new ArrayList<>();
            for (Text val : values){
                VehiclePoint p = new VehiclePoint(val.toString());
                int i = 0;
                while(i < points.size()){
                    List<VehiclePoint> origin = points.get(i);
                    // TODO: 2022/10/13 记得修改check方法
                    int statue = p.check2(origin.get(origin.size()-1));
                    switch (statue){
                        case 0:
                        case 1:
                        case 2:
                            p.setStatue(statue);
                            points.get(i).add(p);
                            break;
                        case 4:
                            origin.get(origin.size()-1).setStatue(4);
                            p.setStatue(4);
                            // 上面的逻辑是不添加10m之内的点 认为是重复点
                            // 下面改为添加
//                            points.get(i).add(p);
                            break;
                        default:
                            break;
                    }
                    if (p.getStatue() != -1){
                        points.sort((v1, v2) -> v2.size()-v1.size());
                        break;
                    }
                    i++;
                }
                if (i == points.size()){
                    p.setStatue(2);
                    List<VehiclePoint> item = new ArrayList<>();
                    item.add(p);
                    points.add(item);
                }
            }
            int count = 0;
            for (List<VehiclePoint> line : points) {
                int size = line.size();
                if (size > 5) {
                    for (VehiclePoint vehiclePoint : line) {
                        context.write(NullWritable.get(), new Text(vehiclePoint.getValue(count)));
                    }
                    count += 1;
                }
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
