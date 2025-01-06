package com.example.hadoopdemo.mapmatching.mapreduce.congestion;

import com.example.hadoopdemo.mapmatching.bean.IntegerPair;
import com.example.hadoopdemo.mapmatching.bean.Line;
import com.example.hadoopdemo.mapmatching.bean.Road;
import com.example.hadoopdemo.mapmatching.jdbc.CRUD;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.security.auth.login.Configuration;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

public class CongestionReducer2 extends Reducer<LongWritable, Text, NullWritable,Text> {
    private ConcurrentHashMap<Long, Float> freeFlowSpeed;
    private ConcurrentHashMap<Long, Long> ling2road;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        freeFlowSpeed = new ConcurrentHashMap<>();
        ling2road = new ConcurrentHashMap<>();
//        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
//        Path path = new Path("hdfs://192.168.1.11:9000/freeflow");
//        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
//        while(files.hasNext()) {
//            LocatedFileStatus file = files.next();
//            Path filePath = file.getPath();
//            FSDataInputStream inputStream = fileSystem.open(filePath);
//            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//            String line;
//            while((line=reader.readLine())!=null){
//                String[] content = line.split("[,]");
//                freeFlowSpeed.put(Long.parseLong(content[0]),Double.parseDouble(content[1]));
//            }
//            reader.close();
//        }
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
        List<PriorityQueue<Float>> speedHour = new ArrayList<>(96);
        for(int i=0;i<96;i++){
            speedHour.add(new PriorityQueue<>());
        }
        for(Text val:values){
            String[] contents = val.toString().split("[,]");
            float speed = Float.parseFloat(contents[4]);
            if(Float.isInfinite(speed) || Float.isNaN(speed) || speed==0) // 速度过小的停留点的过滤，主要看有速度的
                continue;
            Calendar startTime = Calendar.getInstance();
            startTime.setTimeInMillis(Long.parseLong(contents[2]));
            int enterHour = startTime.get(Calendar.HOUR_OF_DAY);
            int enterMinute = startTime.get(Calendar.MINUTE);
            int enter = enterHour*4+enterMinute/15;

            Calendar endTime = Calendar.getInstance();
            endTime.setTimeInMillis(Long.parseLong(contents[3]));
            int exitHour = endTime.get(Calendar.HOUR_OF_DAY);
            int exitMinute = endTime.get(Calendar.MINUTE);
            int exit = exitHour*4+exitMinute/15;
            for(int i = enter;i<=exit;i++){
                speedHour.get(i).add(speed);
            }
        }
        long roadid = ling2road.get(key.get());
        float freeSpeed = freeFlowSpeed.get(roadid); // 1/4-1/5之间差1/20
        StringBuilder sb = new StringBuilder(key.toString());
        sb.append(",").append(freeSpeed);
        StringBuilder sb1 = new StringBuilder();// 将新的指标放在最后
        for(int i=0;i<96;i++){
            PriorityQueue<Float> current = speedHour.get(i);
            int size = current.size();
            float level = 0;
            int nLevel = 4;
            if(size>=5){
                // 先扔1/5的记录
                for(int j=0;j<size/5;j++){
                    current.poll();
                }
                int count = 0;
                // 统计中间的3/5
                for(int j=0;j<size*3/5;j++){
                    if(!current.isEmpty()) {
                        level += current.poll();
                        ++count;
                    }
                }
                // todo 这里注意单位 如果是数据库中的限速单位是km/h 如果是任务算的自由流速度单位是m/s
                float meanSpeed = level/count*3.6f;//m/s->km/h
                level = freeSpeed/meanSpeed-1;
                nLevel = getLevel(meanSpeed, freeSpeed);
            }
            level = Math.min(level, 10);
            level = Math.max(level, 0);
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
//    按小时
//    @Override
//    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        // 自由流速度 3/4-4/5分位数的平均值 暂时不设定为夜间 因为视野也会影响行车速度
//        // 瞬时速度 为每个小时 2/5-4/5分位数之间的平均值
//        List<PriorityQueue<Float>> speedHour = new ArrayList<>(24);
//        for(int i=0;i<24;i++){
//            speedHour.add(new PriorityQueue<>());
//        }
//        int contentSize = 0;
//        for(Text val:values){
//            String[] contents = val.toString().split("[,]");
//            float speed = (Float.parseFloat(contents[4])+Float.parseFloat(contents[5]))/2;
//            if(Float.isInfinite(speed) || Float.isNaN(speed) || speed<0.01) // 速度过小的停留点的过滤，主要看有速度的
//                continue;
//            Calendar startTime = Calendar.getInstance();
//            startTime.setTimeInMillis(Long.parseLong(contents[2]));
//            int enterHour = startTime.get(Calendar.HOUR_OF_DAY);
//
//            Calendar endTime = Calendar.getInstance();
//            endTime.setTimeInMillis(Long.parseLong(contents[3]));
//            int exitHour = endTime.get(Calendar.HOUR_OF_DAY);
//            speedHour.get(enterHour).add(speed);
//            ++contentSize;
//            //  速度可能存在NaN 或 Infinity
////            addData2Array(enterHour, speed, speedTotal, times, freeSpeedList1, freeSpeedList2);
//            if(enterHour!=exitHour){
//                speedHour.get(exitHour).add(speed);
//                ++contentSize;
////                addData2Array(exitHour, speed, speedTotal, times, freeSpeedList1, freeSpeedList2);
//            }
//        }
//        // 计算自由流速度
//        int size1 = Math.max(1, contentSize/4);
//        int size2 = Math.max(1, contentSize/5);
//        PriorityQueue<Float> freeSpeedList1 = new PriorityQueue<>(size1); //记录3/4以上的
//        PriorityQueue<Float> freeSpeedList2 = new PriorityQueue<>(size2); //记录4/5以上的
//
//        for(int i=0;i<24;i++){
//            for(float speed:speedHour.get(i)){
//                add(freeSpeedList1, speed, size1);
//                add(freeSpeedList2, speed, size2);
//            }
//        }
//
//        double freeSpeed1 = freeSpeedList1.stream().mapToDouble(x -> x).sum();
//        double freeSpeed2 = freeSpeedList2.stream().mapToDouble(x -> x).sum();
//        double freeSpeed=(freeSpeed1-freeSpeed2)/contentSize*20; // 1/4-1/5之间差1/20
//        StringBuilder sb = new StringBuilder(key.toString());
//        for(int i=0;i<=23;i++){
//            PriorityQueue<Float> current = speedHour.get(i);
//            int size = current.size();
//            double level = 0;
//            if(size<10){
//                level = 0;// 少于一定监测量 无法推断 认为不堵
//            }else{
//                // 先扔1/5的记录
//                for(int j=0;j<size/5;j++){
//                    current.poll();
//                }
//                int count = 0;
//                // 统计中间的3/5
//                for(int j=0;j<size*3/5;j++){
//                    if(!current.isEmpty()) {
//                        level += current.poll();
//                        ++count;
//                    }
//                }
////                sb.append(",[").append(freeSpeed).append(",").append(level).append(",").append(count).append("]");
//                level = freeSpeed/level*count-1;
//
//            }
//
//            level = Math.min(level, 10);
//            level = Math.max(level, 0);
//            // freeSpeed 和 speedTotal[i] 可能为Infinity
//            sb.append(",").append(level); //INRIX指数
//        }
//        context.write(NullWritable.get(), new Text(sb.toString()));
//    }

//    private void addData2Array(int hour, float speed, float[] speedTotal, int[] times, PriorityQueue<Float> queue, PriorityQueue<Float> queue2){
//        if(Float.isInfinite(speed) || Float.isNaN(speed))
//            return;
//        speedTotal[hour]+=speed;
//        times[hour]++;
//        if(hour>=23 || hour<=4){
//            add(queue, speed);
//        }else{
//            add(queue2, speed);
//        }
//    }
//    private void add(PriorityQueue<Float> queue, float v, int size){
//        if(size<=0)
//            return;
//        if(queue.size()==size){
//            if(v>queue.peek()){
//                queue.poll();
//                queue.offer(v);
//            }
//        }else{
//            queue.offer(v);
//        }
//    }
}
