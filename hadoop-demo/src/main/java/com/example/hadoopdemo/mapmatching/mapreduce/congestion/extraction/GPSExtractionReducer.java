package com.example.hadoopdemo.mapmatching.mapreduce.congestion.extraction;

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

public class GPSExtractionReducer extends Reducer<MyWritable, Text, NullWritable, Text> {

    private Polygon polygon;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        HdfsUtils hdfsUtils = new HdfsUtils();
        hdfsUtils.setHdfs("hdfs://192.168.1.11:9000");
        hdfsUtils.setUser("hadoop");
        String json = hdfsUtils.getJson("/geojson/line_buffer.geojson");
        // 指定GeometryJSON构造器，15位小数
        FeatureJSON featureJSON = new FeatureJSON(new GeometryJSON(15));
        // 读取为FeatureCollection
        FeatureCollection featureCollection = featureJSON.readFeatureCollection(json);
        SimpleFeatureIterator iterator = (SimpleFeatureIterator) featureCollection.features();
        SimpleFeature simpleFeature = iterator.next();
        polygon = (Polygon) simpleFeature.getDefaultGeometry();
    }

    @Override
    protected void reduce(MyWritable key, Iterable<Text> values, Context context) {
        GeometryFactory factory = new GeometryFactory();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

        try {
            for (Text val : values){
                String[] contents = val.toString().split("[,;]");
                Calendar time = Calendar.getInstance();
                String timeStr = contents[0]+contents[1];
                time.setTimeInMillis(format.parse(timeStr).getTime());
                int hour = time.get(Calendar.HOUR_OF_DAY);
                if(hour>4){
                    continue;
                }
                double x = Double.parseDouble(contents[4]);
                double y = Double.parseDouble(contents[5]);
                Point p = factory.createPoint(new Coordinate(x, y));
                if(polygon.contains(p)){
                    context.write(NullWritable.get(), val);
                }
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
