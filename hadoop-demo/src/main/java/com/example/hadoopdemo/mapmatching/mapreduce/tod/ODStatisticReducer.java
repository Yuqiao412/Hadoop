package com.example.hadoopdemo.mapmatching.mapreduce.tod;

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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

public class ODStatisticReducer extends Reducer<MyWritable, Text, NullWritable, Text> {
//    private MultipleOutputs<NullWritable,Text> mos;
    private STRtree tree;
    // 注意当前json的id或者FID可能与shp中objectid不对应，当前杭州TAZ8 id + 1 = objectid
    private final static String regionID = "FID";
    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context) {
        GeometryFactory factory = new GeometryFactory();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

        try {
            Integer last = null;
            com.example.hadoopdemo.mapmatching.bean.Point lastKeyPoint = null;
            com.example.hadoopdemo.mapmatching.bean.Point lastPoint = null;
            Calendar lastKeyTime = null;
            double travelDistance = 0;//每个OD的出行距离
            for (Text val : values){
                String[] contents = val.toString().split("[,;]");
                int weight=Integer.parseInt(contents[8])==1?1:0;
                // 因为要找OD对，所以首先找到起点
                if(last == null && weight != 1){
                    continue;
                }
                double x = Double.parseDouble(contents[4]);
                double y = Double.parseDouble(contents[5]);
                com.example.hadoopdemo.mapmatching.bean.Point p = new com.example.hadoopdemo.mapmatching.bean.Point(x, y);
                if(last==null || last!=weight){

                    String timeStr = contents[0]+contents[1];

                    Calendar time = Calendar.getInstance();
                    time.setTimeInMillis(format.parse(timeStr).getTime());
                    if(weight == 1){
                        lastKeyTime = time;
                        travelDistance = 0;
                    } else if (travelDistance!=0) {
                        // 计算出行时长 单位s
                        long travelTime = (time.getTimeInMillis()-lastKeyTime.getTimeInMillis())/1000;
                        // 计算O点的数据
                        writeData(lastKeyPoint.getX(), lastKeyPoint.getY(), lastKeyTime, 0, travelDistance, travelTime, factory, context);
                        // 计算D点的数据
                        writeData(x, y, time, 1, travelDistance, travelTime, factory, context);
                    }
                    lastKeyPoint = p;
                } else if (weight==1){
                    travelDistance+= Transform.getEuclideanDistance(
                            new com.example.hadoopdemo.mapmatching.bean.Point(x, y),
                            lastPoint);
                }
                lastPoint = p;
                last = weight;
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    private void writeData(double x, double y, Calendar time, int weight, double distance, long during, GeometryFactory factory, Context context) throws IOException, InterruptedException {
        Point p = factory.createPoint(new Coordinate(x, y));
        List features = tree.query(new Envelope(x,x,y,y));
        for(Object o:features){
            SimpleFeature feature = (SimpleFeature) o;
//            MultiPolygon multiPolygon = (MultiPolygon)feature.getDefaultGeometry();
//            Polygon circle = (Polygon)multiPolygon.getGeometryN(0);
            Polygon circle = (Polygon)feature.getDefaultGeometry();
            if(circle.contains(p)){
//                            FeatureId id = feature.getIdentifier();
                String id = feature.getAttribute(regionID).toString();
                int day = time.get(Calendar.DAY_OF_MONTH);
                int hour = time.get(Calendar.HOUR_OF_DAY);
                context.write(NullWritable.get(), new Text(id+","+day+","+hour+","+weight+","+distance+","+during));
//                            mos.write("od", NullWritable.get(), new Text(val), contents[0]); // 按日期输出
            }
        }
    }
    @Override
    protected void setup(Context context) {
        try {
//            mos=new MultipleOutputs<>(context);
            HdfsUtils hdfsUtils = new HdfsUtils();
            hdfsUtils.setHdfs("hdfs://192.168.1.11:9000");
            hdfsUtils.setUser("hadoop");
            String json = hdfsUtils.getJson("/geojson/taz8_buffer.json");
            // 指定GeometryJSON构造器，15位小数
            FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
            // 读取为FeatureCollection
            FeatureCollection featureCollection = featureJSON.readFeatureCollection(json);
            SimpleFeatureIterator iterator = (SimpleFeatureIterator) featureCollection.features();
            SimpleFeature simpleFeature;
            tree=new STRtree();
            while(iterator.hasNext()){
                simpleFeature = iterator.next();
//                MultiPolygon multiPolygon = (MultiPolygon) simpleFeature.getDefaultGeometry();
//                Polygon polygon = (Polygon)multiPolygon.getGeometryN(0);
                Polygon polygon = (Polygon) simpleFeature.getDefaultGeometry();
                tree.insert(polygon.getEnvelopeInternal(), simpleFeature);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        mos.close();
//    }
}
