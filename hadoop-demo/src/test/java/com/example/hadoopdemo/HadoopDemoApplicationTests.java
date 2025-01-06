package com.example.hadoopdemo;

import com.example.hadoopdemo.mapmatching.HMM.Matching;
import com.example.hadoopdemo.mapmatching.a.Search;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.bean.TrajectoryResult;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.jdbc.CRUD;
import com.example.hadoopdemo.mapmatching.mapreduce.*;
import com.example.hadoopdemo.mapmatching.mapreduce.Test.ODTestJob;
import com.example.hadoopdemo.mapmatching.mapreduce.congestion.CongestionJob;
import com.example.hadoopdemo.mapmatching.mapreduce.congestion.extraction.GPSExtractionJob;
import com.example.hadoopdemo.mapmatching.mapreduce.congestion.extraction.LineResultExtractionJob;
import com.example.hadoopdemo.mapmatching.mapreduce.congestion.flowcount.FlowCountTotalJob;
import com.example.hadoopdemo.mapmatching.mapreduce.congestion.freeflow.FreeFlowJob;
import com.example.hadoopdemo.mapmatching.mapreduce.parallel.PairOfInts;
import com.example.hadoopdemo.mapmatching.mapreduce.tod.ODStatisticJob;
import com.example.hadoopdemo.mapmatching.match.RouteRestore;
import com.example.hadoopdemo.mapmatching.utils.ExportUtils;
import com.example.hadoopdemo.utils.HdfsUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

@SpringBootTest
class HadoopDemoApplicationTests {
    @Autowired
    ODJob job3;
    @Autowired
    HdfsUtils hdfsUtils;
    @Autowired
    TrajectoryJob job;
    @Autowired
    CongestionJob job2;
    @Autowired
    ODStatisticJob odStatisticJob;
    @Autowired
    ODTestJob odTestJob;
    @Autowired
    FreeFlowJob freeFlowJob;
    @Autowired
    FlowCountTotalJob flowCountJob;
    @Autowired
    GPSExtractionJob extractionJob1;
    @Autowired
    LineResultExtractionJob extractionJob2;
    @Test
    void extraction() throws InterruptedException, IOException, ClassNotFoundException {
        extractionJob1.execute();
        extractionJob2.execute();
    }
    @Test
    void pretreatment() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\documents\\浮动车数据\\上海\\116542.txt")));
        List<String> values = new ArrayList<>();
        String content = null;
        while((content = reader.readLine())!=null){
            values.add(content);
        }

        List<List<VehiclePoint>> points = new ArrayList<>();
        for (String val : values){
            VehiclePoint p = new VehiclePoint(val);
            int i = 0;
            while(i < points.size()){
                List<VehiclePoint> origin = points.get(i);
                int statue = p.check(origin.get(origin.size()-1));
                switch (statue){
                    case 0:
                    case 1:
                    case 2:
                        p.setStatue(statue);
                        points.get(i).add(p);
                        break;
                    case 4:
                        origin.get(origin.size()-1).setStatue(4);
                        // 上面的逻辑是不添加10m之内的点 认为是重复点
                        // 下面改为添加
                            p.setStatue(4);
                            points.get(i).add(p);
                        break;
                    default:
                        break;
                }
                if (p.getStatue() != -1){
                    points.sort((v1, v2) -> v2.size()-v1.size());
                    break;
                }
                i++;
            }
            if (i == points.size()){
                p.setStatue(2);
                List<VehiclePoint> item = new ArrayList<>();
                item.add(p);
                points.add(item);
            }
        }
        int count = 0;
        for (List<VehiclePoint> line : points) {
            int size = line.size();
            if (size > 5) {
                for (VehiclePoint vehiclePoint : line) {
                   System.out.println(vehiclePoint.getValue(count));
                }
                count += 1;
            }
        }
    }
    @Test
    void contextLoads(){
//        Point p1 = new Point(116.587,39.959,90);
//        Point p2 = new Point(116.591,39.961,90);
//        Point p3 = new Point(116.592,39.958,180);
//        List<Point> points = new ArrayList<>();
//        points.add(p1);
//        points.add(p2);
//        points.add(p3);

        try {
            long time = new Date().getTime();
            Search.initGraph();
            System.out.println(new Date().getTime()-time);
            time = new Date().getTime();
            RoadData.init();
            System.out.println(new Date().getTime()-time);

            List<Map<String, Object>> data = CRUD.read("select * from data order by time", null);
            System.out.println(data.size());
            List<Point> origin = new LinkedList<>();
            for(Map<String, Object> map:data){
                String date = map.get("date").toString()+String.format("%06d", Integer.parseInt(map.get("time").toString()));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//                Point tmp = OffsetCorrection.Transform(new Point(Double.parseDouble(map.get("x").toString()),
//                        Double.parseDouble(map.get("y").toString())));
                Point p = new Point(Double.parseDouble(map.get("x").toString()),
                        Double.parseDouble(map.get("y").toString()),
                        Integer.parseInt(map.get("angle").toString()),
//                        Integer.parseInt(map.get("time").toString()),
                        sdf.parse(date).getTime(), //这版代码得用这个
                        Double.parseDouble(map.get("v").toString()),
                        Integer.parseInt(map.get("status").toString()));
                origin.add(p);
            }
            TrajectoryResult result = RouteRestore.Restore(origin);
			List<Map<String, Object>> maps = new ArrayList<>();
			int i = 1;
			for(Point p :result.getPoints()){
				Map<String, Object> map = new HashMap<>();
				map.put("x",p.getX());
				map.put("y",p.getY());
				map.put("angle",p.getAngle());
                map.put("lineid",p.getLineID());
                map.put("roadid",p.getRoadID());
                map.put("id",i++);
				maps.add(map);
			}
			CRUD.BatchCreate("insert into tmp(x,y,angle,lineid,roadid,id) values(?,?,?,?,?,?)", maps, new String[]{"x","y","angle","lineid","roadid","id"});
            System.out.println(result.getPoints().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    void pretreatmentAndMapMatching() throws Exception {
        //测试代码

        try {
            Search.initGraph();
            RoadData.init();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\liboz\\Desktop\\data.csv")));
//            List<List<VehiclePoint>> points = new ArrayList<>();
//            String content = null;
//            List<List<String>> originRecords = new ArrayList<>();
//            while((content = reader.readLine())!=null){
//                VehiclePoint p = new VehiclePoint(content);
//                int i = 0;
//                while(i < points.size()){
//                    List<VehiclePoint> origin = points.get(i);
//                    int statue = p.check2(origin.get(origin.size()-1));
//                    switch (statue){
//                        case 0:
//                        case 1:
//                        case 2:
//                            p.setStatue(statue);
//                            points.get(i).add(p);
//                            break;
//                        case 4:
//                            origin.get(origin.size()-1).setStatue(4);
//                            p.setStatue(4);
//                            // 上面的逻辑是不添加10m之内的点 认为是重复点
//                            // 下面改为添加
////                            points.get(i).add(p);
//                            break;
//                        default:
//                            break;
//                    }
//                    if (p.getStatue() != -1){
//                        points.sort((v1, v2) -> v2.size()-v1.size());
//                        break;
//                    }
//                    i++;
//                }
//                if (i == points.size()){
//                    p.setStatue(2);
//                    List<VehiclePoint> item = new ArrayList<>();
//                    item.add(p);
//                    points.add(item);
//                }
//            }
//            int count = 0;
//            for (List<VehiclePoint> line : points) {
//                int size = line.size();
//                if (size > 5) {
//                    List<String> item = new ArrayList<>();
//                    for (VehiclePoint vehiclePoint : line) {
//                        item.add(vehiclePoint.getValue(count));
//                    }
//                    originRecords.add(item);
//                    count += 1;
//                }
//            }
//            ExportUtils.export(originRecords, "C:\\Users\\liboz\\Desktop", null);


            List<Point> origin = new ArrayList<>();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            String content;
            while((content = reader.readLine())!=null){
                String[] vs = content.split("[,;]");
                origin.add(new Point(Double.parseDouble(vs[4]), Double.parseDouble(vs[5]),
                        Integer.parseInt(vs[7]), dateFormat.parse(vs[0]+vs[1]).getTime(), Double.parseDouble(vs[6]),
                        Integer.parseInt(vs[8])));
            }
//            int i = 1;
//            List<Map<String, Object>> maps = new ArrayList<>();
//            for(Point p :origin){
//                Map<String, Object> map = new HashMap<>();
//                map.put("x",p.getX());
//                map.put("y",p.getY());
//                map.put("angle",p.getAngle());
//                map.put("lineid",p.getLineID());
//                map.put("roadid",p.getRoadID());
//                map.put("id",i++);
//                maps.add(map);
//            }
//            CRUD.BatchCreate("insert into origin(x,y,angle,lineid,roadid,id) values(?,?,?,?,?,?)", maps, new String[]{"x","y","angle","lineid","roadid","id"});
            TrajectoryResult res = Matching.restoration(origin);
//            maps.clear();
//            i = 1;
//            for(Point p :res.getPoints()){
//                Map<String, Object> map = new HashMap<>();
//                map.put("x",p.getX());
//                map.put("y",p.getY());
//                map.put("angle",p.getAngle());
//                map.put("lineid",p.getLineID());
//                map.put("roadid",p.getRoadID());
//                map.put("id",i++);
//                maps.add(map);
//            }
//            CRUD.BatchCreate("insert into tmp1(x,y,angle,lineid,roadid,id) values(?,?,?,?,?,?)", maps, new String[]{"x","y","angle","lineid","roadid","id"});
            ExportUtils.exportPoint(res, "C:\\Users\\liboz\\Desktop\\res.txt");
            System.out.println(res.getPoints().size());
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    @Test
    void trajectory() throws Exception {
//        hdfsUtils.removeDir("/hangzhou/20171204");
//        hdfsUtils.uploadDir("D:\\documents\\浮动车数据\\杭州\\FCD\\20171204","/hangzhou/20171204");
//        hdfsUtils.uploadDir("D:\\documents\\浮动车数据\\杭州\\FCD\\20171205","/hangzhou/20171205");
//        hdfsUtils.uploadDir("D:\\documents\\浮动车数据\\杭州\\FCD\\20171206","/hangzhou/20171206");
//        hdfsUtils.uploadDir("D:\\documents\\浮动车数据\\杭州\\FCD\\20171207","/hangzhou/20171207");
//        hdfsUtils.uploadDir("D:\\documents\\浮动车数据\\杭州\\FCD\\20171208","/hangzhou/20171208");
//        hdfsUtils.uploadDir("D:\\documents\\浮动车数据\\杭州\\FCD\\20171209","/hangzhou/20171209");
//        hdfsUtils.uploadDir("D:\\documents\\浮动车数据\\杭州\\FCD\\20171210","/hangzhou/20171210");
//        hdfsUtils.uploadDir("D:\\documents\\浮动车数据\\20170630\\20170630","/wuhan");
//        for(int i=20171205;i<=20171210;i++){
//            job.setDate(String.valueOf(i));
//            job.execute();
//        }
        job.setDate("20171204");
        job.execute();

//        Search.initGraph();
//        RoadData.init();
//        List<Map<String, Object>> data = CRUD.read("select * from data order by time", null);
//        List<Point> origin = new LinkedList<>();
//        for(Map<String, Object> map:data){
//            Point p = new Point(Double.parseDouble(map.get("x").toString()),
//                    Double.parseDouble(map.get("y").toString()),
//                    Integer.parseInt(map.get("angle").toString()),
//                    Integer.parseInt(map.get("time").toString()),
//                    Double.parseDouble(map.get("v").toString()),
//                    Integer.parseInt(map.get("status").toString()));
//            origin.add(p);
//        }
//        TrajectoryResult points = RouteRestore.Restore(origin);
//        System.out.println(points.getPoints().size());
    }
    @Test
    void od() throws Exception {
//        hdfsUtils.removeDir("/shanghai1");
//        hdfsUtils.removeDir("/shanghai2");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\taxi\\20150611","/shanghai1");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\taxi\\20150612","/shanghai1");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\taxi\\20150613","/shanghai1");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\taxi\\20150614","/shanghai1");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\taxi\\20150615","/shanghai1");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\taxi\\20150629","/shanghai2");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\taxi\\20150630","/shanghai2");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\taxi\\20150701","/shanghai2");
//        job3.setPathString("shanghai1");
//        job3.execute();
//        job3.setPathString("shanghai2");
        job3.execute();
    }
    @Test
    void congestion() throws Exception {
//        freeFlowJob.execute();
//        ConcurrentHashMap<Long, Float> freeFlowSpeed = new ConcurrentHashMap<>();
//        try {
//            List<Road> roads = CRUD.read("select * from roads",null, Road.class);
//            for(Road road : roads){
//                freeFlowSpeed.put((long)road.getRoadid(), road.getSpeed());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println(freeFlowSpeed.size());
//        for(int i=20171205;i<=20171210;i++){
//            job2.setDate(String.valueOf(i));
//            job2.execute();
//        }
        job2.setDate("20171205");
        job2.execute();

//        // 自由流速度 暂时定为上1/5分位数的平均值 暂时不设定为夜间 因为视野也会影响行车速度
//        String[] values=new String[]{"1,1,1653145369352,1653145369352,30,20,20",
//                "1,1,1653145369352,1653145369352,30,20,20",
//                "1,1,1653145369352,1653145369352,30,20,20",
//                "1,1,1653145369352,1653145369352,20,20,20",
//                "1,1,1653145369352,1653145369352,20,20,20",
//                "1,1,1653145369352,1653145369352,20,20,20",
//                "1,1,1653145369352,1653145369352,20,20,20",
//                "1,1,1653145369352,1653145369352,20,20,20",
//                "1,1,1653145369352,1653145369352,20,20,20",
//                "1,1,1653145369352,1653145369352,20,20,20",
//                "1,1,1653145369352,1653145369352,30,20,20",};
//        int[] count = new int[9];
//        double[] value = new double[9];
//        for(String val:values){
//            String[] contents = val.split("[,]");
//            float speed = Float.parseFloat(contents[4]);
//            // 单位m/s 33.34代表120km/h 上下浮动10% 即为132km/h 36.67m/s
//            if(Float.isInfinite(speed) || Float.isNaN(speed) || speed==0 || speed>=36.67) // 速度过小的停留点的过滤，主要看有速度的
//                continue;
//            int group = (int)Math.min(Math.max(speed*3.6-30, 0)/10, 8);
//            count[group]++;
//            value[group]+=1/speed;
//        }
//        int curCount = 0;
//        double curSpeed = 0;
//        for(int i=8;i>=0;i--){
//            curCount+=count[i];
//            curSpeed+=value[i];
//            // 有10辆车这么走就够了
//            if(curCount>3){
//                break;
//            }
//        }
//        if(curCount==0)
//            return;
//        double freeSpeed=(double)curCount/curSpeed;
//        System.out.println(freeSpeed);
    }
    private void add(PriorityQueue<Float> queue, float v, int size){
        if(size<=0)
            return;
        if(queue.size()==size){
            if(v>queue.peek()){
                queue.poll();
                queue.offer(v);
            }
        }else{
            queue.offer(v);
        }
    }
    @Test
    void tod() throws Exception {
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\20150622","/shanghai/wd");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\20150623","/shanghai/wd");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\20150624","/shanghai/wd");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\20150625","/shanghai/wd");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\20150626","/shanghai/wd");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\20150627","/shanghai/we");
//        hdfsUtils.uploadDir("D:\\workspace\\上海\\20150628","/shanghai/we");
//        hdfsUtils.removeDir("/geojson/subway.json");
        hdfsUtils.upload("C:\\Users\\liboz\\Desktop\\拥堵处理结果\\taz8_buffer.json","/geojson/taz8_buffer.json");
//        odTestJob.execute();
        odStatisticJob.execute();

//        FileFormat.shape2Geojson("D:\\workspace\\上海\\wgs84\\buffer3.shp","D:\\workspace\\上海\\subway.json");
//        String json = FileUtils.readJsonFile("D:\\workspace\\上海\\subway.json");
//        // 指定GeometryJSON构造器，15位小数
//        FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
//        // 读取为FeatureCollection
//        FeatureCollection featureCollection = featureJSON.readFeatureCollection(json);
//        SimpleFeatureIterator iterator = (SimpleFeatureIterator) featureCollection.features();
//        SimpleFeature simpleFeature;
//        STRtree tree=new STRtree();
//        while(iterator.hasNext()){
//            simpleFeature = iterator.next();
//            // 默认是MultiPolygon
//            MultiPolygon multiPolygon = (MultiPolygon) simpleFeature.getDefaultGeometry();
//            Polygon polygon = (Polygon)multiPolygon.getGeometryN(0);
//            tree.insert(polygon.getEnvelopeInternal(), simpleFeature);
//        }
//        System.out.println("pause");
////        GeometryBuilder builder = new GeometryBuilder(DefaultGeographicCRS.WGS84);
//        String[] contents = "20150621,235952,RM,114759,121.334766,31.151793,37,225,0,1,2015-06-22 00:00:00;".split("[,;]");
////        Calendar time = Calendar.getInstance();
////        String timeStr = contents[0]+contents[1];
////        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
////        time.setTimeInMillis(format.parse(timeStr).getTime());
////        int hour = time.get(Calendar.HOUR_OF_DAY);
////        System.out.println(hour);
//        GeometryFactory factory = new GeometryFactory();
//        double x = Double.parseDouble(contents[4]);
//        double y = Double.parseDouble(contents[5]);
//        com.vividsolutions.jts.geom.Point p = factory.createPoint(new Coordinate(x, y));
//        List features = tree.query(new Envelope(x,x,y,y));
//        for(Object o:features){
//            SimpleFeature feature = (SimpleFeature) o;
//            MultiPolygon multiPolygon = (MultiPolygon)feature.getDefaultGeometry();
//            Polygon circle = (Polygon)multiPolygon.getGeometryN(0);
//            if(circle.contains(p)){
////                            FeatureId id = feature.getIdentifier();
//                String id = (String)feature.getAttribute("Allid");
//                System.out.println(id);
//            }
//        }
    }
    @Test
    void flow() throws InterruptedException, IOException, ClassNotFoundException {
////        flowCountJob.execute();
//        hdfsUtils.upload("D:\\BaiduNetdiskDownload\\SH-2018-10-10.csv", "/SH-2018-10-10.csv");
//        odTestJob.execute();
        PairOfInts key1 = new PairOfInts(5,5);
        PairOfInts key2 = new PairOfInts(5,5);
        System.out.println(key1.hashCode());
        System.out.println(key2.hashCode());
        System.out.println(key1.hashCode() == key2.hashCode());
    }
}
