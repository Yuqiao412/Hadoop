package com.example.hadoopdemo.mapmatching.mapreduce.tj;

import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
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
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class RecordsReducer extends Reducer<MyWritable, Text, NullWritable, Text> {
    // hadoop多路径输出的对象
    private MultipleOutputs<NullWritable,Text> mos;

    private final List<STRtree> trees=new ArrayList<>();
    private static final GeometryFactory factory = new GeometryFactory();
    // 获取CRS对象
    private static final CoordinateReferenceSystem wgs84CRS;
    private static final CoordinateReferenceSystem webMercatorCRS;

    static {
        try {
            wgs84CRS = CRS.decode("EPSG:4326");
            webMercatorCRS = CRS.decode("EPSG:3857");
        } catch (FactoryException e) {
            throw new RuntimeException(e);
        }
    }

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
    protected void reduce(MyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Coordinate last = null;
        String lastTime = null;
        for (Text value : values) {
            String[] vals = value.toString().split(",");
            double lon = Double.parseDouble(vals[1])/1000000;
            double lat = Double.parseDouble(vals[2])/1000000;
            String time = dateFormat(Long.parseLong(vals[4]));
            String belongTime = ODMapper.processTimes(lastTime, time); // 这个
            Coordinate current = new Coordinate(lon, lat); // 替换x1, y1为实际坐标值
            if(last != null){
                LineString lineString = factory.createLineString(new Coordinate[]{last, current});
                for(int i=0;i<trees.size();i++){
                    STRtree tree = trees.get(i);
                    String folderName = jsonName[i].substring(jsonName[i].lastIndexOf("/")+1, jsonName[i].lastIndexOf("."));
                    List features = tree.query(lineString.getEnvelopeInternal());
                    for(Object o:features){
                        SimpleFeature feature = (SimpleFeature) o;
                        Double length = getLength(lineString, (Geometry) feature.getDefaultGeometry());
                        if(length != null){
                            String id = feature.getAttribute(attributeID).toString();
                            context.write(NullWritable.get(),new Text(folderName+","+belongTime+","+id+","+length));
                        }
                    }
                }
            }
            last = current;
            lastTime = time;
        }
    }

    private static Double getLength(LineString line, Geometry polygon) {
        try {
            Geometry intersection = line.intersection(polygon);
            if(intersection instanceof LineString || intersection instanceof MultiLineString){
                // 创建MathTransform
                MathTransform transform  = CRS.findMathTransform(wgs84CRS, webMercatorCRS, true);
                // 应用MathTransform进行坐标转换
                Geometry webMercatorGeometry = JTS.transform(intersection, transform);
                return webMercatorGeometry.getLength();
            }
            return null;
        } catch (FactoryException | TransformException e) {
            throw new RuntimeException(e);
        }

    }


    private static String dateFormat(long unixTimestamp){
        // 将Unix时间戳转换为Instant对象
        Instant instant = Instant.ofEpochSecond(unixTimestamp);
        // 将Instant对象转换为LocalDateTime对象（指定时区，例如UTC）
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
        // 定义日期时间格式化器
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        // 将LocalDateTime对象格式化为字符串
        return localDateTime.format(formatter);
    }

    @Override
    protected void setup(Context context) {
        try {
            mos=new MultipleOutputs<>(context);
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

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
