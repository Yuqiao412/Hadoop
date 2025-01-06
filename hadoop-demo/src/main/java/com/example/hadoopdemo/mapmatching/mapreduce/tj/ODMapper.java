package com.example.hadoopdemo.mapmatching.mapreduce.tj;


import com.example.hadoopdemo.utils.HdfsUtils;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.opengis.feature.simple.SimpleFeature;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class ODMapper extends Mapper<LongWritable, Text, Text, Text> {
    // 每个树对应一个json文件
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private final List<STRtree> trees=new ArrayList<>();
    private static final GeometryFactory factory = new GeometryFactory();
    private static final String attributeID = "FID"; // json中用于标识id的字段名
    // 这是我们要统计的json路径，在hdfs上的路径
    private static final String[] jsonName = new String[]{
            "/geojson/1km.json",
            "/geojson/3km.json",
            "/geojson/5km.json",
            "/geojson/500m.json",
            "/geojson/jiaotong.json",
            "/geojson/shequ.json"
    };
    // hdfs的路径记得修改
    private static final String hdfs = "hdfs://192.168.1.11:9000";

    @Override
    protected void map(LongWritable key, Text value, Context context){
        // TODO Auto-generated method stub

        try{
            String[] values = value.toString().split("[,]");
            double ox = Double.parseDouble(values[2])/1000000;
            double oy = Double.parseDouble(values[3])/1000000;
            String ot = values[4]; // 20220629155919
            double dx = Double.parseDouble(values[5])/1000000;
            double dy = Double.parseDouble(values[6])/1000000;
            String dt = values[7];

            // 一条od只属于一个小时
            String time = processTimes(ot, dt);

            Point op = factory.createPoint(new Coordinate(ox, oy));
            Point dp = factory.createPoint(new Coordinate(dx, dy));
            for(int i=0;i<trees.size();i++){
                STRtree tree = trees.get(i);
                String folderName = jsonName[i].substring(jsonName[i].lastIndexOf("/")+1, jsonName[i].lastIndexOf("."));
                List<String> oFeatures = getFeatures(tree, op);
                List<String> dFeatures = getFeatures(tree, dp);
                for(String oFeature : oFeatures){
                    for(String dFeature : dFeatures){
                        context.write(new Text(folderName+"/"+time), new Text(oFeature+","+dFeature));
                    }
                }
            }
        }catch (Exception e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static String processTimes(String time1Str, String time2Str) {
        LocalDateTime time1 = LocalDateTime.parse(time1Str, formatter);
        LocalDateTime time2 = LocalDateTime.parse(time2Str, formatter);

        // 获取两个日期时间的日期部分和小时部分
        LocalDate d1 = time1.toLocalDate();
        LocalTime t1 = time1.toLocalTime().truncatedTo(java.time.temporal.ChronoUnit.HOURS);
        LocalDate d2 = time2.toLocalDate();
        LocalTime t2 = time2.toLocalTime().truncatedTo(java.time.temporal.ChronoUnit.HOURS);
        // 检查时间是否相等
        if (d1.equals(d2) && t1.equals(t2)) {
            return time1.format(DateTimeFormatter.ofPattern("yyyyMMddHH"));
        }

        // 检查时间差是否为1小时 返回在小时内持续最久的那个小时
        long hoursBetween = ChronoUnit.HOURS.between(t1, t2);
        if (hoursBetween == 1) {
            // 计算dateTime1距离下一个小时的时间
            long minutesUntilNextHour1 = ChronoUnit.MINUTES.between(time1, time1.truncatedTo(ChronoUnit.HOURS).plusHours(1));
            // 计算dateTime2在当前小时内的时间
            long minutesIntoCurrentHour2 = ChronoUnit.MINUTES.between(time2.truncatedTo(ChronoUnit.HOURS), time2);
            // 根据条件返回对象
            if (minutesUntilNextHour1 > minutesIntoCurrentHour2) {
                return time1.format(DateTimeFormatter.ofPattern("yyyyMMddHH"));
            } else {
                return time2.format(DateTimeFormatter.ofPattern("yyyyMMddHH"));
            }
        }

        // 否则返回第二个时间（年月日时）
        return time2.truncatedTo(ChronoUnit.HOURS).format(DateTimeFormatter.ofPattern("yyyyMMddHH"));
    }

    public static List<String> getFeatures(STRtree tree, Point p) {
        List<String> result = new ArrayList<>();
        List features = tree.query(new Envelope(p.getX(), p.getX(), p.getY(), p.getY()));
        for(Object o:features){
            SimpleFeature feature = (SimpleFeature) o;
            if(feature.getDefaultGeometry() instanceof Polygon){
                Polygon polygon = (Polygon) feature.getDefaultGeometry();
                if(polygon.contains(p)){
                    result.add(feature.getAttribute(attributeID).toString());
                }
            }else{
                MultiPolygon multiPolygon = (MultiPolygon) feature.getDefaultGeometry();
                if(multiPolygon.contains(p)){
                    result.add(feature.getAttribute(attributeID).toString());
                }
            }
        }
        return result;
    }

    @Override
    protected void setup(Context context) {
        try {
            HdfsUtils hdfsUtils = new HdfsUtils();
            hdfsUtils.setHdfs(hdfs);
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
                    Envelope envelope;
                    if(simpleFeature.getDefaultGeometry() instanceof Polygon){
                        envelope = ((Polygon) simpleFeature.getDefaultGeometry()).getEnvelopeInternal();
                    }else{
                        MultiPolygon multiPolygon = (MultiPolygon) simpleFeature.getDefaultGeometry();
                        envelope = multiPolygon.getEnvelopeInternal();
                    }
                    tree.insert(envelope, simpleFeature);
                }
                tree.build();
                trees.add(tree);
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
