package com.example.hadoopdemo.mapmatching.mapreduce.parallel;


import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class HMMReduce extends Reducer<MyWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        Map<CandidateKey, CandidatePoint> lastMap = new HashMap<>();
        Map<CandidateKey, CandidatePoint> curMap = new HashMap<>();
        boolean isFirstPoint = true;
        // vehicleID, lastTime, x, y, lineid, roadid, score, time, x, y, lineid, roadid, score, linearDis, totalDis
        Long lastTime = null;
        for(Text val:values){
            String[] contents = val.toString().split("[,]");
            CandidatePoint cur = new CandidatePoint(contents, true);
            if(lastTime == null){
                lastTime = cur.getTime();
            }else if(lastTime != cur.getTime()){
                lastTime = cur.getTime();
                isFirstPoint = false;
                lastMap = curMap;
                curMap = new HashMap<>();
            }
            double linearDis = Double.parseDouble(contents[13]);
            double totalDis = Double.parseDouble(contents[14]);
            CandidatePoint last = null;
            if(isFirstPoint){
                last = new CandidatePoint(contents, false);

            }else{
                CandidateKey lastKey = new CandidateKey(Integer.parseInt(contents[4]), Integer.parseInt(contents[5]));
                if(lastMap.containsKey(lastKey)){
                    last = lastMap.get(lastKey);
                }
            }
            if(last!=null){
                cur.setLast(last);
                // 按照加法来搞 距离先按普通的来 试试效率
                cur.setScore(last.score+cur.getScore()*Math.min(linearDis, totalDis)/Math.max(linearDis, totalDis));
                CandidateKey curKey = new CandidateKey(cur);
                if(!curMap.containsKey(curKey) || curMap.get(curKey).getScore()<cur.getScore()){
                    curMap.put(curKey, cur);
                }
            }
        }
        CandidatePoint maxPoint = null;
        for(CandidatePoint p : curMap.values()){
            if(maxPoint==null || maxPoint.getScore()<p.getScore()){
                maxPoint = p;
            }
        }
        while(maxPoint!=null){
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(maxPoint.getTime());
            String sb = calendar.get(Calendar.YEAR) + padding(calendar.get(Calendar.MONTH) + 1) + padding(Calendar.DAY_OF_MONTH) +
                    "," + padding(Calendar.HOUR_OF_DAY) + padding(Calendar.MINUTE) + padding(Calendar.SECOND) +
                    "," + key.getVehicleID().toString() +
                    "," + maxPoint.getX() +
                    "," + maxPoint.getY() +
                    "," + maxPoint.getLineId() +
                    "," + maxPoint.getRoadId();
            context.write(NullWritable.get(), new Text(sb));
            maxPoint = maxPoint.getLast();
        }

    }
    private String padding(int number){
        return String.format("%02d", number);
    }
    private static class CandidatePoint{
        private long time;
        private double x;
        private double y;
        private int lineId;
        private int roadId;
        private double score;
        private CandidatePoint last;

        public CandidatePoint(String[] contents, boolean last){
            int offset = last?6:0;
            this.time = Long.parseLong(contents[1+offset]);
            this.x = Double.parseDouble(contents[2+offset]);
            this.y = Double.parseDouble(contents[3+offset]);
            this.lineId = Integer.parseInt(contents[4+offset]);
            this.roadId = Integer.parseInt(contents[5+offset]);
            this.score = Double.parseDouble(contents[6+offset]);
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public double getX() {
            return x;
        }

        public void setX(double x) {
            this.x = x;
        }

        public double getY() {
            return y;
        }

        public void setY(double y) {
            this.y = y;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        public CandidatePoint getLast() {
            return last;
        }

        public void setLast(CandidatePoint last) {
            this.last = last;
        }

        public int getLineId() {
            return lineId;
        }

        public void setLineId(int lineId) {
            this.lineId = lineId;
        }

        public int getRoadId() {
            return roadId;
        }

        public void setRoadId(int roadId) {
            this.roadId = roadId;
        }
    }
    private static class CandidateKey{
        private int lineId;
        private int roadId;

        public CandidateKey(int lineId, int roadId) {
            this.lineId = lineId;
            this.roadId = roadId;
        }
        public CandidateKey(CandidatePoint p) {
            this.lineId = p.getLineId();
            this.roadId = p.getRoadId();
        }

        public int getLineId() {
            return lineId;
        }

        public void setLineId(int lineId) {
            this.lineId = lineId;
        }

        public int getRoadId() {
            return roadId;
        }

        public void setRoadId(int roadId) {
            this.roadId = roadId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lineId, roadId);
        }
    }
}
