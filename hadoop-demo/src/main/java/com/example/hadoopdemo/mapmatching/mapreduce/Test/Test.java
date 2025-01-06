package com.example.hadoopdemo.mapmatching.mapreduce.Test;

import com.example.hadoopdemo.mapmatching.utils.FileUtils;
import com.example.hadoopdemo.mapmatching.utils.Transform;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.opengis.feature.simple.SimpleFeature;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Test {
    private static STRtree tree;
    private final static String regionID = "FID";

    public static void main(String[] args) throws IOException, ParseException {
        System.out.println(1 | 1<<1);
//        String json = FileUtils.readJsonFile("C:\\Users\\liboz\\Desktop\\line_buffer.geojson");
//        // 指定GeometryJSON构造器，15位小数
//        FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
//        // 读取为FeatureCollection
//        FeatureCollection featureCollection = featureJSON.readFeatureCollection(json);
//        SimpleFeatureIterator iterator = (SimpleFeatureIterator) featureCollection.features();
//        SimpleFeature simpleFeature = iterator.next();
//        Polygon polygon = (Polygon) simpleFeature.getDefaultGeometry();
//        String timeStr = "20171205230501";
//        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
//        Calendar time = Calendar.getInstance();
//        time.setTimeInMillis(format.parse(timeStr).getTime());
//        System.out.println(time.getTime());


//        InputStreamReader isr = new InputStreamReader(new FileInputStream("C:\\Users\\liboz\\Desktop\\part-r-00002(2).txt")); // 读取
//        // 创建字符流缓冲区
//        BufferedReader br = new BufferedReader(isr); // 缓冲
//        Set<Integer> daySet = new HashSet<>();
//        String val;
//        while((val = br.readLine())!=null) {
//            String[] content = val.split("[,]");
//            int day = Integer.parseInt(content[1]);
//            if(day>10)
//                System.out.println(val);
//            daySet.add(day);
//        }
//        for(Integer d : daySet){
//            System.out.println(d);
//        }


//        String json = FileUtils.readJsonFile("C:\\Users\\liboz\\Desktop\\拥堵处理结果\\taz8.json");
//        FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
//        // 读取为FeatureCollection
//        FeatureCollection featureCollection = featureJSON.readFeatureCollection(json);
//        SimpleFeatureIterator iterator = (SimpleFeatureIterator) featureCollection.features();
//        SimpleFeature simpleFeature;
//        tree = new STRtree();
//        while (iterator.hasNext()) {
//            simpleFeature = iterator.next();
////                MultiPolygon multiPolygon = (MultiPolygon) simpleFeature.getDefaultGeometry();
////                Polygon polygon = (Polygon)multiPolygon.getGeometryN(0);
//            Polygon polygon = (Polygon) simpleFeature.getDefaultGeometry();
//            tree.insert(polygon.getEnvelopeInternal(), simpleFeature);
//        }
//        InputStreamReader isr = new InputStreamReader(new FileInputStream("C:\\Users\\liboz\\Desktop\\ceshi.txt")); // 读取
//        // 创建字符流缓冲区
//        BufferedReader br = new BufferedReader(isr); // 缓冲
//        GeometryFactory factory = new GeometryFactory();
//        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
//        Integer last = null;
//        com.example.hadoopdemo.mapmatching.bean.Point lastKeyPoint = null;
//        com.example.hadoopdemo.mapmatching.bean.Point lastPoint = null;
//        Calendar lastKeyTime = null;
//        double travelDistance = 0;//每个OD的出行距离
//        String val;
//        while((val = br.readLine())!=null) {
//            String[] contents = val.split("[,;]");
//            int weight = Integer.parseInt(contents[8]) == 1 ? 1 : 0;
//            // 因为要找OD对，所以首先找到起点
//            if (last == null && weight != 1) {
//                continue;
//            }
//            double x = Double.parseDouble(contents[4]);
//            double y = Double.parseDouble(contents[5]);
//            com.example.hadoopdemo.mapmatching.bean.Point p = new com.example.hadoopdemo.mapmatching.bean.Point(x, y);
//            if (last == null || last != weight) {
//
//                String timeStr = contents[0] + contents[1];
//
//                Calendar time = Calendar.getInstance();
//                time.setTimeInMillis(format.parse(timeStr).getTime());
//                // 一会考虑一下其他状态
//                if (weight == 1) {
//                    lastKeyTime = time;
//                    travelDistance = 0;
//                } else {
//                    // 计算出行时长 单位s
//                    long travelTime = (time.getTimeInMillis() - lastKeyTime.getTimeInMillis()) / 1000;
//                    // 计算O点的数据
//                    writeData(lastKeyPoint.getX(), lastKeyPoint.getY(), lastKeyTime, 0, travelDistance, travelTime, factory);
//                    // 计算D点的数据
//                    writeData(x, y, time, 1, travelDistance, travelTime, factory);
//                }
//                lastKeyPoint = p;
//            } else if (weight == 1) {
//                travelDistance += Transform.getEuclideanDistance(
//                        new com.example.hadoopdemo.mapmatching.bean.Point(x, y),
//                        lastPoint);
//            }
//            lastPoint = p;
//            last = weight;
//        }
    }

    private static void writeData(double x, double y, Calendar time, int weight, double distance, long during, GeometryFactory factory){
        Point p = factory.createPoint(new Coordinate(x, y));
        List features = tree.query(new Envelope(x, x, y, y));
        for (Object o : features) {
            SimpleFeature feature = (SimpleFeature) o;
//            MultiPolygon multiPolygon = (MultiPolygon)feature.getDefaultGeometry();
//            Polygon circle = (Polygon)multiPolygon.getGeometryN(0);
            Polygon circle = (Polygon) feature.getDefaultGeometry();
            if (circle.contains(p)) {
//                            FeatureId id = feature.getIdentifier();
                String id = feature.getAttribute(regionID).toString();
                int day = time.get(Calendar.DAY_OF_MONTH);
                int hour = time.get(Calendar.HOUR_OF_DAY);
                System.out.println(id + "," + day + "," + hour + "," + weight + "," + distance + "," + during);
//                            mos.write("od", NullWritable.get(), new Text(val), contents[0]); // 按日期输出
            }
        }
    }
}
