package com.example.hadoopdemo.mapmatching.mapreduce.tod;

import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import com.example.hadoopdemo.utils.HdfsUtils;
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

public class ODCountReducer extends Reducer<MyWritable, Text, NullWritable, Text> {

    private STRtree tree;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        HdfsUtils hdfsUtils = new HdfsUtils();
        hdfsUtils.setHdfs("hdfs://192.168.0.201:9000");
        hdfsUtils.setUser("hadoop");
        String json = hdfsUtils.getJson("/geojson/subway.json");
        // 指定GeometryJSON构造器，15位小数
        FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
        // 读取为FeatureCollection
        FeatureCollection featureCollection = featureJSON.readFeatureCollection(json);
        SimpleFeatureIterator iterator = (SimpleFeatureIterator) featureCollection.features();
        SimpleFeature simpleFeature;
        tree=new STRtree();
        while(iterator.hasNext()){
            simpleFeature = iterator.next();
            MultiPolygon multiPolygon = (MultiPolygon) simpleFeature.getDefaultGeometry();
            Polygon polygon = (Polygon)multiPolygon.getGeometryN(0);
//            Polygon polygon = (Polygon) simpleFeature.getDefaultGeometry();
            tree.insert(polygon.getEnvelopeInternal(), simpleFeature);
        }
    }

    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context) {
        GeometryFactory factory = new GeometryFactory();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

        try {
            Integer last = null;
            for (Text val : values){
                String[] contents = val.toString().split("[,;]");
                int weight=Integer.parseInt(contents[8])==1?1:0;
                if(last!=null && last!=weight){
//                    context.write(NullWritable.get(), new Text(val));
                    double x = Double.parseDouble(contents[4]);
                    double y = Double.parseDouble(contents[5]);
                    Point p = factory.createPoint(new Coordinate(x, y));

                    Calendar time = Calendar.getInstance();
                    String timeStr = contents[0]+contents[1];
                    time.setTimeInMillis(format.parse(timeStr).getTime());
                    int hour = time.get(Calendar.HOUR_OF_DAY);

                    List features = tree.query(new Envelope(x,x,y,y));
                    for(Object o:features){
                        SimpleFeature feature = (SimpleFeature) o;
                        MultiPolygon multiPolygon = (MultiPolygon)feature.getDefaultGeometry();
                        Polygon circle = (Polygon)multiPolygon.getGeometryN(0);
                        if(circle.contains(p)){
//                            FeatureId id = feature.getIdentifier();
                            String id = feature.getAttribute("Allid").toString();
                            context.write(NullWritable.get(), new Text(id+","+hour+","+weight));
                        }
                    }
                }
                last = weight;
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
