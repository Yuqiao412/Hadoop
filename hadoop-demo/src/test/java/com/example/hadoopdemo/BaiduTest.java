package com.example.hadoopdemo;

import com.example.hadoopdemo.mapmatching.mapreduce.baidu.ODFirstJob;
import com.example.hadoopdemo.mapmatching.mapreduce.baidu.ODSecondJob;
import com.example.hadoopdemo.mapmatching.mapreduce.baidu.TrajectoryPoint;
import com.example.hadoopdemo.mapmatching.utils.FileUtils;
import com.example.hadoopdemo.mapmatching.utils.Transform;
import com.example.hadoopdemo.utils.HdfsUtils;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.junit.jupiter.api.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@SpringBootTest
public class BaiduTest {
    @Autowired
    ODFirstJob job;
    @Autowired
    ODSecondJob job2;
    @Autowired
    HdfsUtils hdfsUtils;

    // 代码全是盲打的 还没测试哈 20230718
    @Test
    void extraction() throws InterruptedException, IOException, ClassNotFoundException {
        // 指定输入输出时，需检查hadoop中是否存在同名的文件夹，程序执行默认会删除输出文件夹中的所有数据
        String inputPath = "/wuhantest"; // 原始数据的位置，文件格式为txt
//        hdfsUtils.uploadDir("D:\\data", inputPath);// 可以通过这个代码，将本地文件夹传输到hdfs上
        String outputPath1 = "/od"; // job的输出文件夹 输出结果为每个点所在的格网id以及其他信息，详见ODReducer
        String outputPath2 = "/result";// job2的输出文件夹 输出结果为每个格网的上下车信息，详见CountReducer

        // job负责过滤异常点，并统计每个点所在的格网id，可以同时处理多天的数据，注意集群硬盘空间
        // 格网数据定义在ODReducer中，支持1到多个
        job.setInputPath(inputPath);
        job.setOutputPath(outputPath1);
        job.execute();
//        // job处理完之后，会对不同的json文件的结果生成不同的文件夹，详见ODReducer
//        String[] jsonName = new String[]{
//                "1km",
//                "3km",
//                "5km",
//                "500m",
//                "jiaotong",
//                "shequ"
//        };
//        // job2负责统计每个格网的上下车点数，需要针对不同的统计单元（json）分别统计
//        // 目前是按天统计，如需修改，可以参见详见CountReducer
//        for(String name:jsonName){
//            job2.setInputPath(outputPath1+"/"+name);
//            job2.setOutputPath(outputPath2+"/"+name);
//            job2.execute();
//        }
    }

    @Test
    void logicTest() throws IOException, ParseException, InterruptedException {
        String ymd = 2021 + String.format("%02d", 12) + String.format("%02d", 6);
        System.out.println(ymd);
//        String json = FileUtils.readJsonFile("C:\\Users\\liboz\\Desktop\\1km.json");
//        // 指定GeometryJSON构造器，15位小数
//        FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
//        // 读取为FeatureCollection
//        FeatureCollection featureCollection = featureJSON.readFeatureCollection(json);
//        SimpleFeatureIterator iterator = (SimpleFeatureIterator) featureCollection.features();
//        SimpleFeature simpleFeature;
//        STRtree tree = new STRtree();
//        while (iterator.hasNext()) {
//            simpleFeature = iterator.next();
//            Polygon polygon;
//            if (simpleFeature.getDefaultGeometry() instanceof Polygon) {
//                polygon = (Polygon) simpleFeature.getDefaultGeometry();
//            } else {
//                MultiPolygon multiPolygon = (MultiPolygon) simpleFeature.getDefaultGeometry();
//                polygon = (Polygon) multiPolygon.getGeometryN(0);
//            }
//            tree.insert(polygon.getEnvelopeInternal(), simpleFeature);
//        }
//        tree.build();
//        System.out.println("str success");
//
//        List<List<TrajectoryPoint>> points = new ArrayList<>();
//        // 执行过滤和分割逻辑
//        String[] values = new String[]{"20221206,235942,data1,a874dc2e8d1c075a,114.331732,30.497058,42,282,11,1",
//                "20221206,235943,data1,a874dc2e8d1c075a,114.331613,30.497084,43,282,11,1",
//                "20221206,235944,data1,a874dc2e8d1c075a,114.331491,30.497110,42,282,11,1",
//                "20221206,235945,data1,a874dc2e8d1c075a,114.331372,30.497135,41,282,11,1",
//                "20221206,235946,data1,a874dc2e8d1c075a,114.331255,30.497160,41,282,11,1",
//                "20221206,235947,data1,a874dc2e8d1c075a,114.331138,30.497185,40,282,11,1",
//                "20221206,235948,data1,a874dc2e8d1c075a,114.331024,30.497210,41,282,11,1",
//                "20221206,235949,data1,a874dc2e8d1c075a,114.330908,30.497235,39,282,11,1",
//                "20221206,235950,data1,a874dc2e8d1c075a,114.330798,30.497260,38,282,11,1",
//                "20221206,235951,data1,a874dc2e8d1c075a,114.330691,30.497283,37,281,11,1",
//                "20221206,235952,data1,a874dc2e8d1c075a,114.330586,30.497305,36,281,11,1",
//                "20221206,235953,data1,a874dc2e8d1c075a,114.330485,30.497324,33,281,11,1",
//                "20221206,235954,data1,a874dc2e8d1c075a,114.330391,30.497343,31,281,11,1",
//                "20221206,235955,data1,a874dc2e8d1c075a,114.330301,30.497361,31,282,11,1",
//                "20221206,235956,data1,a874dc2e8d1c075a,114.330213,30.497380,30,282,11,1",
//                "20221206,235957,data1,a874dc2e8d1c075a,114.330129,30.497399,28,270,11,1"};
//        for (String val : values) {
//            TrajectoryPoint point = new TrajectoryPoint(val);
//            int i;
//            for (i = 0; i < points.size(); i++) {
//                List<TrajectoryPoint> trajectory = points.get(i);
//                TrajectoryPoint last = trajectory.get(trajectory.size() - 1);
//                double distance = Transform.getDistance(point.getMercatorPoint(), last.getMercatorPoint());
//                long time = point.getTime() - last.getTime();
//                // 只有当 时间在15分钟以内，距离在150米以内，速度不超过120km/h
//                if ((time <= 1000 * 60 * 15 && !(distance > 150)) && ((time != 0 && distance / time * 60 * 60 <= 120) || (time == 0 && distance <= 50))) {
//                    trajectory.add(point);
//                    break;
//                }
//            }
//            if (i >= points.size()) {
//                List<TrajectoryPoint> trajectory = new ArrayList<>();
//                trajectory.add(point);
//                points.add(trajectory);
//                points.sort((o1, o2) -> o2.size() - o1.size());
//            }
//        }
//        // 统计上下车点，对于长度小于5的轨迹直接撇了
//        // 每个轨迹的第一个点认为是上车点 最后一个点认为是下车点
//        // 由于原始数据在时间上可能不连续，所以每辆车的第一个上车点和最后一个下车点暂时不算
//        // 由于预处理逻辑可能是的一个轨迹 O点时间比其他轨迹的时间都早，但D点却比其他轨迹时间晚，因此先汇总再筛选
//        List<TrajectoryPoint> pickups = new ArrayList<>();
//        List<TrajectoryPoint> dropoffs = new ArrayList<>();
//        for (List<TrajectoryPoint> trajectory : points) {
//            if (trajectory.size() > 5) {
//                pickups.add(trajectory.get(0));
//                dropoffs.add(trajectory.get(trajectory.size() - 1));
//            }
//        }
//        pickups.sort(Comparator.comparingLong(TrajectoryPoint::getTime));
//        dropoffs.sort(Comparator.comparingLong(TrajectoryPoint::getTime));
//        for (TrajectoryPoint pickup : pickups) {
//            cover(pickup, 0, tree);
//        }
//        for (TrajectoryPoint dropoff : dropoffs) {
//            cover(dropoff, 1, tree);
//        }
    }

    private void cover(TrajectoryPoint point, int status, STRtree tree) throws IOException, InterruptedException {
        GeometryFactory factory = new GeometryFactory();
        double x = point.getPoint().getX();
        double y = point.getPoint().getY();
        long time = point.getTime();
        Point p = factory.createPoint(new Coordinate(x, y));
        List features = tree.query(new Envelope(x, x, y, y));
        for (Object o : features) {
            SimpleFeature feature = (SimpleFeature) o;
            Polygon circle;
            if(feature.getDefaultGeometry() instanceof Polygon){
                circle = (Polygon) feature.getDefaultGeometry();
            }else{
                MultiPolygon multiPolygon = (MultiPolygon) feature.getDefaultGeometry();
                circle = (Polygon) multiPolygon.getGeometryN(0);
            }

            if (circle.contains(p)) {
                String id = feature.getAttribute("FID").toString();
                System.out.println(id + "," + time + "," + status);
            }
        }
    }
}
