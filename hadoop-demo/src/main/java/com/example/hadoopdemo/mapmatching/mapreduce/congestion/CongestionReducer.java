package com.example.hadoopdemo.mapmatching.mapreduce.congestion;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class CongestionReducer extends Reducer<LongWritable, Text, NullWritable,Text> {
    // 第一种方法 计算的自由流速度较大 计算的瞬时速度 没有忽略临时停靠等低速点
//    private static final int QUEUE_SIZE = 10;
//    @Override
//    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        float[] speedTotal = new float[24];// 记录总速度
//        int[] times = new int[24];// 记录总记录数
//        PriorityQueue<Float> freeSpeedList1 = new PriorityQueue<>(QUEUE_SIZE); //记录所有 23点到凌晨4点的速度值 小顶堆
//        PriorityQueue<Float> freeSpeedList2 = new PriorityQueue<>(QUEUE_SIZE); //怕没有值 将非上述时间段内的值 放在这个里面
//        for(Text val:values){
//            String[] contents = val.toString().split("[,]");
//            Calendar startTime = Calendar.getInstance();
//            startTime.setTimeInMillis(Long.parseLong(contents[2]));
//            int enterHour = startTime.get(Calendar.HOUR_OF_DAY);
//
//
//            Calendar endTime = Calendar.getInstance();
//            endTime.setTimeInMillis(Long.parseLong(contents[3]));
//            int exitHour = endTime.get(Calendar.HOUR_OF_DAY);
//
//            float speed = (Float.parseFloat(contents[4])+Float.parseFloat(contents[5]))/2;
//            //  速度可能存在NaN 或 Infinity
//            addData2Array(enterHour, speed, speedTotal, times, freeSpeedList1, freeSpeedList2);
//            if(enterHour!=exitHour){
//                addData2Array(exitHour, speed, speedTotal, times, freeSpeedList1, freeSpeedList2);
//            }
//        }
//        double freeSpeed1 = freeSpeedList1.stream().mapToDouble(x -> x).sum();
//        double freeSpeed2 = freeSpeedList2.stream().mapToDouble(x -> x).sum();;
//        double freeSpeed=freeSpeedList1.size()==0?
//                (freeSpeedList2.size()==0?0:freeSpeed2/freeSpeedList2.size()):
//                freeSpeed1/freeSpeedList1.size();
//        StringBuilder sb = new StringBuilder(key.toString());
//        for(int i=0;i<=23;i++){
//            double level = speedTotal[i]==0 || times[i]<=QUEUE_SIZE?0:freeSpeed/speedTotal[i]*times[i] - 1;
//            level = Math.min(level, 10);
//            level = Math.max(level, 0);
//            // freeSpeed 和 speedTotal[i] 可能为Infinity
//            sb.append(",").append(level); //INRIX指数
//        }
//        context.write(NullWritable.get(), new Text(sb.toString()));
//    }
    // 每15分钟统计一次
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 自由流速度 3/4-4/5分位数的平均值 暂时不设定为夜间 因为视野也会影响行车速度
        // 瞬时速度 为每刻 2/5-4/5分位数之间的平均值
        List<PriorityQueue<Float>> speedHour = new ArrayList<>(96);
        for(int i=0;i<96;i++){
            speedHour.add(new PriorityQueue<>());
        }
        List<Float> all = new ArrayList<>(); // 由所有记录的速度来求自由流速度，不能使用分时间段之后增加的速度，可能会造成速度过大

        for(Text val:values){
            String[] contents = val.toString().split("[,]");
//            float speed = (Float.parseFloat(contents[5])+Float.parseFloat(contents[6]))/2;
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
            speedHour.get(enter).add(speed);
            all.add(speed);
            if(enterHour!=exitHour){
                for(int i = enter;i<=exit;i++){
                    speedHour.get(i).add(speed);
                }
            }
        }
        int contentSize = all.size();
        // 计算自由流速度
        int size1 = Math.max(1, contentSize/4);
        int size2 = Math.max(1, contentSize/5);
        PriorityQueue<Float> freeSpeedList1 = new PriorityQueue<>(size1); //记录3/4以上的
        PriorityQueue<Float> freeSpeedList2 = new PriorityQueue<>(size2); //记录4/5以上的

        for(float speed:all){
            add(freeSpeedList1, speed, size1);
            add(freeSpeedList2, speed, size2);
        }

        double freeSpeed1 = freeSpeedList1.stream().mapToDouble(x -> x).sum();
        double freeSpeed2 = freeSpeedList2.stream().mapToDouble(x -> x).sum();
        double freeSpeed=(freeSpeed1-freeSpeed2)/contentSize*20; // 1/4-1/5之间差1/20
        StringBuilder sb = new StringBuilder(key.toString());
        sb.append(",").append(freeSpeed);
        for(int i=0;i<96;i++){
            PriorityQueue<Float> current = speedHour.get(i);
            int size = current.size();
            double level = 0;
            if(size<5){
                level = 0;// 少于一定监测量 无法推断 认为不堵
            }else{
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
//                sb.append(",[").append(freeSpeed).append(",").append(level).append(",").append(count).append("]");
                level = freeSpeed/level*count-1;

            }

            level = Math.min(level, 10);
            level = Math.max(level, 0);
            // freeSpeed 和 speedTotal[i] 可能为Infinity
            sb.append(",").append(level); //INRIX指数
        }
        context.write(NullWritable.get(), new Text(sb.toString()));
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
    private void add(PriorityQueue<Float> queue, float v, int size){
        if(size<=0)
            return;
        if(queue.size()==size){
            if(v>queue.peek()){
                queue.poll();
                queue.offer(v);
            }
        }else{
            queue.offer(v);
        }
    }
}
