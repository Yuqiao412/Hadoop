package com.example.hadoopdemo.mapmatching.mapreduce;

import com.example.hadoopdemo.mapmatching.a.Search;
import com.example.hadoopdemo.mapmatching.bean.LineResult;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.bean.TrajectoryResult;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.match.RouteRestore;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class MatchingReducer extends Reducer<MyWritable, Text, NullWritable,Text> {
    private MultipleOutputs<NullWritable,Text> mos;
    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context){
        try {
            List<Point> origin = new ArrayList<>();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (Text val : values) {
                String[] vs = val.toString().split("[,;]");
                origin.add(new Point(Double.parseDouble(vs[4]), Double.parseDouble(vs[5]),
                        Float.parseFloat(vs[7]), dateFormat.parse(vs[10]).getTime(), Double.parseDouble(vs[6]),
                        Integer.parseInt(vs[8])));
            }
            TrajectoryResult points = RouteRestore.OnlyMatching(origin);
            for(Point point:points.getPoints()){
                mos.write("points", NullWritable.get(), new Text(key.vehicleID+","+point.getX()+","+
                        point.getY()+","+point.getTime()+","+
                        point.getVelocity()+","+point.getAngle()+","+point.getStatus()+","
                        +point.getLineID()+","+point.getRoadID()),"points/points");
            }
//            for(LineResult line:points.getLines()){
//                mos.write("lines", NullWritable.get(), new Text(line.getLineID()+","+line.getRoadID()+","+
//                        line.getStartTime()+","+line.getEndTime()+","+
//                        line.getStartSpeed()+","+line.getEndSpeed()),"lines/lines");
//            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    @Override
    protected void setup(Context context) {
        try {
            Search.initGraph();
            RoadData.init();
            mos=new MultipleOutputs<>(context);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
