package com.example.hadoopdemo.mapmatching.mapreduce;

import com.example.hadoopdemo.mapmatching.HMM.Matching;
import com.example.hadoopdemo.mapmatching.a.Search;
import com.example.hadoopdemo.mapmatching.bean.LineResult;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.bean.TrajectoryResult;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.match.RouteRestore;
import com.example.hadoopdemo.mapmatching.utils.Transform;
import com.example.hadoopdemo.utils.HdfsUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class TrajectoryReducer extends Reducer<MyWritable, Text, NullWritable,Text> {
    private MultipleOutputs<NullWritable,Text> mos;
//    private static int type = 0; // 为了找异常数据的代码
    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context){
        try {
//            if(type==3){
//                return;
//            }
            List<Point> origin = new ArrayList<>();
//            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 杭州这里数据需要修改
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
//            List<Text> originText = new ArrayList<>();
            for (Text val : values) {
//                originText.add(val);
                String[] vs = val.toString().split("[,;]");
//                origin.add(new Point(Double.parseDouble(vs[4]), Double.parseDouble(vs[5]),
//                        Integer.parseInt(vs[7]), dateFormat.parse(vs[10]).getTime(), Double.parseDouble(vs[6]),
//                        Integer.parseInt(vs[8])));
                origin.add(new Point(Double.parseDouble(vs[4]), Double.parseDouble(vs[5]),
                        Float.parseFloat(vs[7]), format.parse(vs[0]+vs[1]).getTime(), Double.parseDouble(vs[6]),
                        Integer.parseInt(vs[8])));
            }
//            TrajectoryResult points = RouteRestore.MapMatching(origin);
            TrajectoryResult points = Matching.restoration(origin);
            for(Point point:points.getPoints()){
                mos.write("points", NullWritable.get(), new Text(key.vehicleID+","+point.getX()+","+
                        point.getY()+","+point.getTime()+","+
                        point.getVelocity()+","+point.getStatus()),"points/points");
            }
            for(LineResult line:points.getLines()){
                mos.write("lines", NullWritable.get(), new Text(line.getLineID()+","+line.getRoadID()+","+
                        line.getStartTime()+","+line.getEndTime()+","+line.getMeanSpeed()+","+
                        line.getStartSpeed()+","+line.getEndSpeed()),"lines/lines");
            }
//            // 输出一个异常值看看
//            type |= points.getErrorType();
//            for(Text t: originText){
//                mos.write("errors", NullWritable.get(), t,"errors/errors");
//            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    @Override
    protected void setup(Reducer<MyWritable, Text, NullWritable, Text>.Context context) {
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
