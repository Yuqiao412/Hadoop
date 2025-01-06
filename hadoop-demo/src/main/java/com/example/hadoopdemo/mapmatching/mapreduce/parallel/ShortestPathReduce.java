package com.example.hadoopdemo.mapmatching.mapreduce.parallel;

import ch.hsr.geohash.GeoHash;
import com.example.hadoopdemo.mapmatching.HMM.CommonMethod;
import com.example.hadoopdemo.mapmatching.HMM.bean.CandidatePoint;
import com.example.hadoopdemo.mapmatching.HMM.bean.DriveSegment;
import com.example.hadoopdemo.mapmatching.a.ASparkResult;
import com.example.hadoopdemo.mapmatching.a.Search;
import com.example.hadoopdemo.mapmatching.a.Vertex;
import com.example.hadoopdemo.mapmatching.bean.Line;
import com.example.hadoopdemo.mapmatching.bean.Pair;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.bean.Road;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.utils.PathCommon;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ShortestPathReduce extends Reducer<PairOfInts, Text, NullWritable, Text> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            RoadData.init();
            Search.initGraph();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(PairOfInts key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        try {
            long maxTime = 0;
            List<String[]> valuesCopy = new ArrayList<>();
            for(Text val:values){
                String[] contents = val.toString().split(",");
                long time = PathCommon.getSecondGap(Long.parseLong(contents[1]), Long.parseLong(contents[7]));
                maxTime = Math.max(maxTime, time);
                valuesCopy.add(contents);
            }
            //加上time 保证每个点的效率！！
            ASparkResult<Vertex> aSparkResult = Search.searchNodeAndSolve(key.getFirst(), key.getSecond(), maxTime);//A*
            if(aSparkResult==null)
                return;
            Vertex last = null;
            double distance = 0;
            List<Float> speed = new ArrayList<>();
            for(Vertex vertex:aSparkResult.getRoute()){
                if(last != null){
                    Integer roadid = RoadData.getRoadByNodes(last.getVertexID(),vertex.getVertexID());
//                    boolean direction = true; //正向
                    if(roadid==null){
                        roadid = RoadData.getRoadByNodes(vertex.getVertexID(),last.getVertexID());
//                        direction = false;
                    }
                    Road road = RoadData.getRoadByID(roadid);
                    if(road != null){
//                        List<Line> segments = new LinkedList<>(RoadData.getLinesByRoad(roadid));
//                        new DriveSegment(roadid,road.getDistance(),road.getSpeed()*1000/3600,segments,direction);
                        speed.add(road.getSpeed()*1000/3600);
                        distance += road.getDistance();
                    }
                }
                last = vertex;
            }
            for(String[] contents:valuesCopy){
                long time = PathCommon.getSecondGap(Long.parseLong(contents[1]), Long.parseLong(contents[7]));
                double totalDis = Double.parseDouble(contents[3]) + Double.parseDouble(contents[9]) + distance;
                float meanSpeed = (float)(totalDis/time);
                if(meanSpeed<=120/3.6f){
                    // 计算时间特征
                    double a = 0, b =0, c=0;// a是分子，b是分母的左半部分，c是分母的右半部分
                    for(float spd: speed){
                        a+=spd*meanSpeed;
                        b+=Math.pow(spd,2);
                        c+=Math.pow(meanSpeed,2);
                    }
                    double ft =  a/Math.sqrt(b)/Math.sqrt(c);
                    // vehicleID, lastTime, x, y, lineid, roadid, score, time, x, y, lineid, roadid, score, linearDis
                    String sb = contents[0] +
                            "," + contents[1] +
                            "," + contents[4] +
                            "," + contents[5] +
                            "," + contents[6] +
                            "," + contents[7] +
                            "," + contents[8] +
                            "," + contents[9] +
                            "," + contents[12] +
                            "," + contents[13] +
                            "," + contents[14] +
                            "," + contents[15] +
                            "," + (Double.parseDouble(contents[16]) * ft) +
                            "," + contents[17] +
                            "," + totalDis;
                    context.write(NullWritable.get(), new Text(sb));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
