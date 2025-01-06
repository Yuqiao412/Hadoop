package com.example.hadoopdemo.mapmatching.mapreduce.parallel;

import ch.hsr.geohash.GeoHash;
import com.example.hadoopdemo.mapmatching.HMM.CommonMethod;
import com.example.hadoopdemo.mapmatching.HMM.bean.CandidatePoint;
import com.example.hadoopdemo.mapmatching.a.Search;
import com.example.hadoopdemo.mapmatching.bean.Line;
import com.example.hadoopdemo.mapmatching.bean.Pair;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.utils.PathCommon;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CandidateReduce extends Reducer<Text, Text, NullWritable, Text> {
    private static final double envelope = 0.002;//搜索半径 大约200m 包含geohash约80米
    private static final double mu = 0;// 计算观测概率的期望
    private static final double fSigma = 50;// 计算观测概率的标准差 浮动车数据算法改为50 以减少距离的比重
    private static final double directionalMu = 0;
    private static final double fDirectionalSigma = Math.toRadians(30); // 浮动车数据算法改为30 以减少角度的比重

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            RoadData.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        GeoHash hash = GeoHash.fromGeohashString(key.toString());
        double lon = hash.getOriginatingPoint().getLongitude();
        double lat = hash.getOriginatingPoint().getLatitude();
        List<Line> lines = RoadData.getLinesByExtent(lon - envelope, lon + envelope, lat - envelope, lat + envelope);
        for(Text value:values){
            String[] vs = value.toString().split("[,]");
            double x = Double.parseDouble(vs[4]);
            double y = Double.parseDouble(vs[5]);
            float angle = Float.parseFloat(vs[7]);
            Point point =  new Point(x, y, angle);

            Map<Integer, CandidatePoint> candidatePoints = new HashMap<>();
            Map<Integer, CandidatePoint> other = new HashMap<>();
            for(Line line:lines){
                Pair<Float, CandidatePoint> pedal = CommonMethod.CalculatePoint4Parallel(point, line);
                if(pedal.getA()>60)//角度差大于60的直接pass snail代码中取60
                    continue;
                int roadid = pedal.getB().getLine().getRoadid();
                if(pedal.getB().getR()>=0 && pedal.getB().getR()<=1){
                    candidatePoints.put(roadid, pedal.getB());
                }
                else if(pedal.getB().getHeight()<30 || (pedal.getB().getR()>=-0.1 && pedal.getB().getR()<=1.1)) {
                    if(!candidatePoints.containsKey(roadid) || candidatePoints.get(roadid).getHeight()>pedal.getB().getHeight()){
                        candidatePoints.put(roadid, pedal.getB());
                    }
                }
                else if(!other.containsKey(roadid) || other.get(roadid).getHeight()>pedal.getB().getHeight()){
                    other.put(roadid, pedal.getB());
                }
            }
            if(candidatePoints.isEmpty()){
                candidatePoints.putAll(other);
            }
            if(!candidatePoints.isEmpty()){
                StringBuilder sb = new StringBuilder(vs[0]).append(",").append(vs[1]).append(",").append(vs[3]).append(",").append(vs[4]).append(",").append(vs[5]);
                for(CandidatePoint p:candidatePoints.values()){
                    Pair<Pair<Integer, Double>, Pair<Integer, Double>> endpoints = PathCommon.getODAndDistance(p.getCandidatePoint());
                    double observationProbability = CommonMethod.gaussianFunction(p.getHeight(),mu,fSigma)*
                            CommonMethod.directionAnalysis(p,directionalMu,fDirectionalSigma);
                    assert endpoints != null;
                    sb.append(";").append(endpoints.getA().getA())
                            .append(",").append(endpoints.getA().getB())
                            .append(",").append(endpoints.getB().getA())
                            .append(",").append(endpoints.getB().getB())
                            .append(",").append(p.getCandidatePoint().getX())
                            .append(",").append(p.getCandidatePoint().getY())
                            .append(",").append(p.getCandidatePoint().getLineID())
                            .append(",").append(p.getCandidatePoint().getRoadID())
                            .append(",").append(observationProbability);
                }
                context.write(NullWritable.get(), new Text(sb.toString()));
            }
        }


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
