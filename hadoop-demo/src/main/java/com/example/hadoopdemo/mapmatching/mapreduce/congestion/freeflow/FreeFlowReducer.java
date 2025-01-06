package com.example.hadoopdemo.mapmatching.mapreduce.congestion.freeflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 如果道路数据由限速，推荐使用限速，因为某些道路上，车的行驶速度会比限速高很多
 * 如果需要计算自由流，后续推荐用非高峰时期的众数作为自由流速度(https://www.csdn.net/tags/MtTaMg3sMjU3NDQ3LWJsb2cO0O0O.html)
 * a) 至少连续观测1个月。
 * b) 使用算术平均聚合每个时间间隔在观测时间段内的所有有效样本的行程速度的平均值，如果有效样本达不到一定阈值，这个时间间隔的行程速度不纳入后续计算。
 * c) 根据时间段中速度最大的累计 4 小时样本速度的均值作为该link的自由流速度。
 * 目前采用全天的速度（如：取速度最高的两个分区中有值的进行平均，则得到的结果较高，如果取众数，则可能因为拥堵导致结果过低）
 * 或采用晚上的速度（可能没有监测结果）都不是十分准确
 */
public class FreeFlowReducer extends Reducer<LongWritable, Text, NullWritable,Text> {
    // 决定按速度分区，不能按百分比，这样容易出问题
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        boolean onlyNight = context.getConfiguration().getBoolean("freeflow_onlynight", false);
        // 40以下,40-50,50-60,60-70,70-80,80-90,90-100,100-110,110以上
        int[] all_count = new int[9];
        // 记录速度倒数之和 平均速度用(double)size/topSpeed.stream().mapToDouble(x -> 1/x).sum();来求
        double[] all_value = new double[9];

        int[] night_count = new int[9];
        double[] night_value = new double[9];
        for(Text val:values){
            String[] contents = val.toString().split("[,]");

            Calendar startTime = Calendar.getInstance();
            startTime.setTimeInMillis(Long.parseLong(contents[2]));
            int enterHour = startTime.get(Calendar.HOUR_OF_DAY);

            Calendar endTime = Calendar.getInstance();
            endTime.setTimeInMillis(Long.parseLong(contents[3]));
            int exitHour = endTime.get(Calendar.HOUR_OF_DAY);

            float speed = Float.parseFloat(contents[4]);
            // 单位m/s 33.34代表120km/h 不再浮动 （上下浮动10% 即为132km/h 36.67m/s）
            if(Float.isInfinite(speed) || Float.isNaN(speed) || speed<=0 || speed>=33.34) // 速度过小的停留点的过滤，主要看有速度的
                continue;
            int group = (int)Math.min(Math.max(speed*3.6-30, 0)/10, 8);
            if(enterHour<=5 || enterHour>=23 || exitHour<=5 || exitHour>=23) {
                night_count[group]++;
                night_value[group]+=1/speed;
            }
            all_count[group]++;
            all_value[group]+=1/speed;
        }
        Double dayFreeSpeed = getFreeSpeed(all_count, all_value);
        Double nightFreeSpeed = getFreeSpeed(night_count, night_value);
        if(dayFreeSpeed==null)
            return;
        // 当前计算的自由流速度 可能还存在部分道路值过小的情况 需要后续再处理即可
        context.write(NullWritable.get(), new Text(key+","+dayFreeSpeed+","+(nightFreeSpeed==null?"null":nightFreeSpeed)+","+
                all_count[0]+","+all_count[1]+","+all_count[2]+","+all_count[3]+","+all_count[4]+","+all_count[5]+","+all_count[6]+","+all_count[7]+","+all_count[8]+","+
                night_count[0]+","+night_count[1]+","+night_count[2]+","+night_count[3]+","+night_count[4]+","+night_count[5]+","+night_count[6]+","+night_count[7]+","+night_count[8]));
    }
    private Double getFreeSpeed(int[] count, double[] value){
        int curCount = 0;
        double curSpeed = 0;
        int groupCount = 0;
        for(int i=8;i>=0;i--){
            curCount+=count[i];
            curSpeed+=value[i];
            // 除了40以下的，用速度最高的2组来平均吧
            if(count[i]!=0){
                ++groupCount;
            }
            if((i==1 && groupCount>0) || groupCount>=2){
                break;
            }
        }
        if(curCount==0)
            return null;
        return (double)curCount/curSpeed;
    }
//    // 汇总所有日期计算自由流速度 定义为所有速度（或夜间速度）的前1/5的平均值
//    @Override
//    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        boolean onlyNight = context.getConfiguration().getBoolean("freeflow_onlynight", false);
//        // 自由流速度 暂时定为上1/5分位数的平均值 暂时不设定为夜间 因为视野也会影响行车速度
//        int threshold = 5;
//        List<Float> all = new ArrayList<>(); // 由所有记录的速度来求自由流速度，不能使用分时间段之后增加的速度，可能会造成速度过大
//        for(Text val:values){
//            String[] contents = val.toString().split("[,]");
//            if(onlyNight){
//                Calendar startTime = Calendar.getInstance();
//                startTime.setTimeInMillis(Long.parseLong(contents[2]));
//                int enterHour = startTime.get(Calendar.HOUR_OF_DAY);
//
//                Calendar endTime = Calendar.getInstance();
//                endTime.setTimeInMillis(Long.parseLong(contents[3]));
//                int exitHour = endTime.get(Calendar.HOUR_OF_DAY);
//                if(enterHour>5 && enterHour<23 && exitHour>5 && exitHour<23)
//                    continue;
//            }
//            float speed = Float.parseFloat(contents[4]);
//            // 单位m/s 33.34代表120km/h 上下浮动10% 即为132km/h 36.67m/s
//            if(Float.isInfinite(speed) || Float.isNaN(speed) || speed==0 || speed>=36.67) // 速度过小的停留点的过滤，主要看有速度的
//                continue;
//            all.add(speed);
//        }
//        int contentSize = all.size();
//        if(contentSize==0)
//            return;
//        // 计算自由流速度
//        // 保证至少有10个点来统计自由流速度
//        int size = Math.max(10, contentSize/threshold);
//        // 如果总数都不够10个点，那就有多少来多少
//        size = Math.min(size, contentSize);
//
//        PriorityQueue<Float> topSpeed = new PriorityQueue<>(size);
//
//        for(float speed:all){
//            add(topSpeed, speed, size);
//        }
//        double freeSpeed=(double)size/topSpeed.stream().mapToDouble(x -> 1/x).sum();
//        context.write(NullWritable.get(), new Text(key+","+freeSpeed));
//    }
//    按天计算自由流速度 按小时统计拥堵指数
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
