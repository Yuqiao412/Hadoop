package com.example.hadoopdemo.mapmatching.mapreduce.parallel;

import com.example.hadoopdemo.mapmatching.HMM.CommonMethod;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import com.example.hadoopdemo.mapmatching.match.Common;
import com.example.hadoopdemo.mapmatching.utils.Transform;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ODGroupReduce extends Reducer<MyWritable, Text, NullWritable, Text> {
    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<TmpCandidatePoint> last = new ArrayList<>();
        double lastX = 0, lastY = 0;
        long lastTime = 0;
        for(Text val:values){
            String[] contents = val.toString().split("[;]");
            long time = Long.parseLong(contents[0]);
            double x = Double.parseDouble(contents[1]);
            double y = Double.parseDouble(contents[2]);
            List<TmpCandidatePoint> candidatePoints = new ArrayList<>();
            double linearDis = Transform.getEuclideanDistance(new Point(lastX, lastY), new Point(x, y));
            for(int i=1;i<contents.length;i++){
                TmpCandidatePoint cur = new TmpCandidatePoint(contents[i]);
                for(TmpCandidatePoint l:last){
                    String sb = key.getVehicleID().toString() +
                            "," + lastTime +
                            "," + l.getEndNodeId() +
                            "," + l.getEndDis() +
                            "," + l.getX() +
                            "," + l.getY() +
                            "," + l.getLineId() +
                            "," + l.getRoadId() +
                            "," + l.getScore() +
                            "," + time +
                            "," + cur.getStartNodeId() +
                            "," + cur.getStartDis() +
                            "," + cur.getX() +
                            "," + cur.getY() +
                            "," + cur.getLineId() +
                            "," + cur.getRoadId() +
                            "," + cur.getScore() +
                            "," + linearDis;
                    context.write(NullWritable.get(), new Text(sb));
                }
                candidatePoints.add(cur);
            }
            last = candidatePoints;
            lastX = x;
            lastY = y;
        }
    }
    private class TmpCandidatePoint{
        private int startNodeId;
        private double startDis;
        private int endNodeId;
        private double endDis;
        private double x;
        private double y;
        private int lineId;
        private int roadId;
        private double score;

        public TmpCandidatePoint(){

        }
        public TmpCandidatePoint(String val){
            String[] contents = val.split("[,]");
            this.startNodeId = Integer.parseInt(contents[0]);
            this.startDis = Double.parseDouble(contents[1]);
            this.endNodeId = Integer.parseInt(contents[2]);
            this.endDis = Double.parseDouble(contents[3]);
            this.x = Double.parseDouble(contents[4]);
            this.y = Double.parseDouble(contents[5]);
            this.lineId = Integer.parseInt(contents[6]);
            this.roadId = Integer.parseInt(contents[7]);
            this.score = Double.parseDouble(contents[8]);
        }

        public int getStartNodeId() {
            return startNodeId;
        }

        public void setStartNodeId(int startNodeId) {
            this.startNodeId = startNodeId;
        }

        public double getStartDis() {
            return startDis;
        }

        public void setStartDis(double startDis) {
            this.startDis = startDis;
        }

        public int getEndNodeId() {
            return endNodeId;
        }

        public void setEndNodeId(int endNodeId) {
            this.endNodeId = endNodeId;
        }

        public double getEndDis() {
            return endDis;
        }

        public void setEndDis(double endDis) {
            this.endDis = endDis;
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
}
