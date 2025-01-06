package com.example.hadoopdemo.mapmatching.mapreduce.congestion;

import com.example.hadoopdemo.mapmatching.bean.Line;
import com.example.hadoopdemo.mapmatching.bean.Road;
import com.example.hadoopdemo.mapmatching.jdbc.CRUD;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

public class CongestionReducer2ByHour extends Reducer<LongWritable, Text, NullWritable,Text> {
    private ConcurrentHashMap<Long, Float> freeFlowSpeed;
    private ConcurrentHashMap<Long, Long> ling2road;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        freeFlowSpeed = new ConcurrentHashMap<>();
        ling2road = new ConcurrentHashMap<>();
        try {
            List<Road> roads = CRUD.read("select * from roads",null, Road.class);
            for(Road road : roads){
                freeFlowSpeed.put((long)road.getRoadid(), road.getSpeed());
            }
            List<Line> lines =  CRUD.read("select * from lines",null, Line.class);
            for(Line line : lines){
                ling2road.put((long)line.getLineid(), (long)line.getRoadid());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 每15分钟统计一次，每次统计如果小于5辆车的记录，则认为监测量不足，不予统计
        // 否则，将排除行驶最快的1/5和行驶最慢的1/5的数据，用中间数据的平均值作为当前道路的交通状态
        List<PriorityQueue<Float>> speedHour = new ArrayList<>(24);
        for(int i=0;i<24;i++){
            speedHour.add(new PriorityQueue<>());
        }
        for(Text val:values){
            String[] contents = val.toString().split("[,]");
//            float speed = Float.parseFloat(contents[4]);
            float meanSpeed = Float.parseFloat(contents[4]) * 3.6f;// km/h
            float actualSpeed = (Float.parseFloat(contents[5])+Float.parseFloat(contents[6]))/2; // km/h
            float speed = meanSpeed<actualSpeed || meanSpeed>120?actualSpeed:meanSpeed; // km/h
            if(Float.isInfinite(speed) || Float.isNaN(speed) || speed==0) // 速度过小的停留点的过滤，主要看有速度的
                continue;
            Calendar startTime = Calendar.getInstance();
            startTime.setTimeInMillis(Long.parseLong(contents[2]));
            int enter = startTime.get(Calendar.HOUR_OF_DAY);

            Calendar endTime = Calendar.getInstance();
            endTime.setTimeInMillis(Long.parseLong(contents[3]));
            int exit = endTime.get(Calendar.HOUR_OF_DAY);
            for(int i = enter;i<=exit;i++){
                speedHour.get(i).add(speed);
            }
        }
        long roadid = ling2road.get(key.get());
        float freeSpeed = freeFlowSpeed.get(roadid);
        StringBuilder sb = new StringBuilder(key.toString());
        sb.append(",").append(freeSpeed);
        StringBuilder sb1 = new StringBuilder();// 将新的指标放在最后
        for(int i=0;i<24;i++){
            PriorityQueue<Float> current = speedHour.get(i);
            int size = current.size();
            float level = -1; // 默认是个无效值
            int nLevel = 0; // 默认是个无效值
            if(size>=10){
                // 先扔4/5的记录 速度小的异常值扔掉
                for(int j=0;j<size/5;j++){
                    current.poll();
                }
                int count = 0;
                float totalSpeed = 0;
                // 统计中间的3/5 todo 考虑将速度大的保留
                for(int j=0;j<size*3/5;j++){
                    if(!current.isEmpty()) {
                        totalSpeed += current.poll();
                        ++count;
                    }
                }
//                while(!current.isEmpty()) {
//                    totalSpeed += current.poll();
//                    ++count;
//                }
                float meanSpeed = totalSpeed/count;
                level = freeSpeed/meanSpeed-1;
                level = Math.min(level, 10);
                level = Math.max(level, 0);
                nLevel = getLevel(meanSpeed, freeSpeed);
            }
            // freeSpeed 和 speedTotal[i] 可能为Infinity
            sb.append(",").append(level); //INRIX指数
            sb1.append(",").append(nLevel);//拥堵等级
        }
        context.write(NullWritable.get(), new Text(sb.toString()+sb1.toString()));
    }

    /**
     * 《道路交通拥堵度评价方法》（GAT 1152020）
     * @param speed 平均速度
     * @param limit 道路限速
     * @return 拥堵等级 1最严重拥堵
     */
    private int getLevel(float speed, float limit){
        // 限速80以上的以公路或城市快速度平均行驶属于与交通拥堵度的对应关系为准
        // 限速80以下的的以城市主干路和次干路的平均行驶速度与交通拥堵度的对应关系为准
        if(limit>=120){
            if(speed>=70)
                return 4;
            else if(speed>=50)
                return 3;
            else if(speed>=30)
                return 2;
            else
                return 1;
        } else if(limit>=110){
            if(speed>=65)
                return 4;
            else if(speed>=45)
                return 3;
            else if(speed>=25)
                return 2;
            else
                return 1;
        } else if(limit>=100){
            if(speed>=60)
                return 4;
            else if(speed>=40)
                return 3;
            else if(speed>=20)
                return 2;
            else
                return 1;
        } else if(limit>=90){
            if(speed>=55)
                return 4;
            else if(speed>=35)
                return 3;
            else if(speed>=20)
                return 2;
            else
                return 1;
        } else if(limit>=80){
            if(speed>=45)
                return 4;
            else if(speed>=30)
                return 3;
            else if(speed>=20)
                return 2;
            else
                return 1;
        } else if(limit>=70){
            if(speed>=40)
                return 4;
            else if(speed>=30)
                return 3;
            else if(speed>=20)
                return 2;
            else
                return 1;
        } else if(limit>=60){
            if(speed>=35)
                return 4;
            else if(speed>=30)
                return 3;
            else if(speed>=20)
                return 2;
            else
                return 1;
        } else if(limit>=50){
            if(speed>=30)
                return 4;
            else if(speed>=25)
                return 3;
            else if(speed>=15)
                return 2;
            else
                return 1;
        } else if(limit>=40){
            if(speed>=25)
                return 4;
            else if(speed>=20)
                return 3;
            else if(speed>=15)
                return 2;
            else
                return 1;
        } else {
            if(speed>=25)
                return 4;
            else if(speed>=20)
                return 3;
            else if(speed>=10)
                return 2;
            else
                return 1;
        }

    }
}
