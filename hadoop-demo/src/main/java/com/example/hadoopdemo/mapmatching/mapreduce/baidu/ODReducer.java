package com.example.hadoopdemo.mapmatching.mapreduce.baidu;

import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import com.example.hadoopdemo.mapmatching.utils.Transform;
import com.example.hadoopdemo.utils.HdfsUtils;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ODReducer extends Reducer<MyWritable, Text, NullWritable, Text> {
    // hadoop多路径输出的对象
    private MultipleOutputs<NullWritable,Text> mos;
    // 每个树对应一个json文件
    private List<STRtree> trees=new ArrayList<>();
    private static GeometryFactory factory = new GeometryFactory();
    private static final String attributeID = "FID";
    private static String[] jsonName = new String[]{
            "/geojson/1km.json",
            "/geojson/3km.json",
            "/geojson/5km.json",
            "/geojson/500m.json",
            "/geojson/jiaotong.json",
            "/geojson/shequ.json"
    };
    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context) {
        List<List<TrajectoryPoint>> points = new ArrayList<>();
        try {
            // 执行过滤和分割逻辑
            for (Text val : values){
                TrajectoryPoint point = new TrajectoryPoint(val.toString());
                int i;
                for(i=0;i<points.size();i++){
                    List<TrajectoryPoint> trajectory = points.get(i);
                    TrajectoryPoint last = trajectory.get(trajectory.size()-1);
                    double distance = Transform.getDistance(point.getMercatorPoint(), last.getMercatorPoint());
                    long time = point.getTime()-last.getTime();
                    // 只有当 时间在15分钟以内，距离在150米以内，速度不超过120km/h
                    if ((time <= 1000 * 60 * 15 && !(distance > 150)) && ((time!=0 && distance / time * 60 * 60 <= 120) || (time==0 && distance <= 50))) {
                        trajectory.add(point);
                        break;
                    }
                }
                if(i>=points.size()){
                    List<TrajectoryPoint> trajectory = new ArrayList<>();
                    trajectory.add(point);
                    points.add(trajectory);
                    points.sort((o1, o2) -> o2.size()-o1.size());
                }
            }
            // 统计上下车点，对于长度小于5的轨迹直接撇了
            // 每个轨迹的第一个点认为是上车点 最后一个点认为是下车点
            // 由于原始数据在时间上可能不连续，所以每辆车的第一个上车点和最后一个下车点暂时不算 (忽略这个判断， 忽略以下的注释)
            // 由于预处理逻辑可能是的一个轨迹 O点时间比其他轨迹的时间都早，但D点却比其他轨迹时间晚，因此先汇总再筛选
            List<TrajectoryPoint> pickups = new ArrayList<>();
            List<TrajectoryPoint> dropoffs = new ArrayList<>();
            for(List<TrajectoryPoint> trajectory : points){
                if(trajectory.size()>5){
                    pickups.add(trajectory.get(0));
                    dropoffs.add(trajectory.get(trajectory.size()-1));
                }
            }
            pickups.sort(Comparator.comparingLong(TrajectoryPoint::getTime));
            dropoffs.sort(Comparator.comparingLong(TrajectoryPoint::getTime));
            for (TrajectoryPoint pickup : pickups) {
                cover(pickup, 0);
            }
            for (TrajectoryPoint dropoff : dropoffs) {
                cover(dropoff, 1);
            }

        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    private void cover(TrajectoryPoint point, int status) throws IOException, InterruptedException {
        double x = point.getPoint().getX();
        double y = point.getPoint().getY();
//        Calendar time = Calendar.getInstance();
//        time.setTimeInMillis(point.getTime());
//        int hour = time.get(Calendar.HOUR_OF_DAY);
        long time = point.getTime();
//        String day = point.getDay();
        Point p = factory.createPoint(new Coordinate(x, y));
        for(int i=0;i<trees.size();i++){
            STRtree tree = trees.get(i);
            String folderName = jsonName[i].substring(jsonName[i].lastIndexOf("/")+1, jsonName[i].lastIndexOf("."));
            List features = tree.query(new Envelope(x,x,y,y));
            for(Object o:features){
                SimpleFeature feature = (SimpleFeature) o;
                Polygon circle;
                if(feature.getDefaultGeometry() instanceof Polygon){
                    circle = (Polygon) feature.getDefaultGeometry();
                }else{
                    MultiPolygon multiPolygon = (MultiPolygon) feature.getDefaultGeometry();
                    circle = (Polygon) multiPolygon.getGeometryN(0);
                }
                if(circle.contains(p)){
//                            FeatureId id = feature.getIdentifier();
                    String id = feature.getAttribute(attributeID).toString();
//                    context.write(NullWritable.get(), new Text(id+","+hour+","+status));
//                    mos.write("od", NullWritable.get(), new Text(id+","+time+","+status), folderName+"/"+day); // 按日期输出
                    mos.write("od", NullWritable.get(), new Text(id+","+time+","+status), folderName+"/od"); // 只按json类型输出
                }
            }
        }


    }
    @Override
    protected void setup(Context context) {
        try {
            mos=new MultipleOutputs<>(context);
            HdfsUtils hdfsUtils = new HdfsUtils();
            hdfsUtils.setHdfs("hdfs://192.168.1.11:9000");
            hdfsUtils.setUser("hadoop");
            for(String name:jsonName){
                String json = hdfsUtils.getJson(name);
                // 指定GeometryJSON构造器，15位小数
                FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
                // 读取为FeatureCollection
                FeatureCollection featureCollection = featureJSON.readFeatureCollection(json);
                SimpleFeatureIterator iterator = (SimpleFeatureIterator) featureCollection.features();
                SimpleFeature simpleFeature;
                STRtree tree=new STRtree();
                while(iterator.hasNext()){
                    simpleFeature = iterator.next();
                    Polygon polygon;
                    if(simpleFeature.getDefaultGeometry() instanceof Polygon){
                        polygon = (Polygon) simpleFeature.getDefaultGeometry();
                    }else{
                        MultiPolygon multiPolygon = (MultiPolygon) simpleFeature.getDefaultGeometry();
                        polygon = (Polygon)multiPolygon.getGeometryN(0);
                    }
                    tree.insert(polygon.getEnvelopeInternal(), simpleFeature);
                }
                tree.build();
                trees.add(tree);
            }

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
