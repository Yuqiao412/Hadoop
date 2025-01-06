package com.example.hadoopdemo.mapmatching.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.vividsolutions.jts.geom.*;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.geometry.primitive.Point;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class FileFormat {
    /**
     * geojson转换为shp文件
     *
     * @param jsonPath
     * @param shpPath
     * @return
     */
    public static Map<String, String> geojson2Shape(String jsonPath, String shpPath) {
        Map<String, String> map = new HashMap<>();
        GeometryJSON gjson = new GeometryJSON();
        try {
            String strJson = FileUtils.readJsonFile(jsonPath);
            JSONObject json = JSONObject.parseObject(strJson);
            JSONArray features = (JSONArray) json.get("features");
            JSONObject feature0 = JSONObject.parseObject(features.get(0).toString());
            System.out.println(feature0.toString());
            String strType = ((JSONObject) feature0.get("geometry")).getString("type");

            Class<?> geoType = null;
            switch (strType) {
                case "Point":
                    geoType = Point.class;
                case "MultiPoint":
                    geoType = MultiPoint.class;
                case "LineString":
                    geoType = LineString.class;
                case "MultiLineString":
                    geoType = MultiLineString.class;
                case "Polygon":
                    geoType = Polygon.class;
                case "MultiPolygon":
                    geoType = MultiPolygon.class;
            }
            //创建shape文件对象
            File file = new File(shpPath);
            Map<String, Serializable> params = new HashMap<>();
            params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
            ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);
            //定义图形信息和属性信息
            SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
            tb.setCRS(DefaultGeographicCRS.WGS84);
            tb.setName("shapefile");
            tb.add("the_geom", geoType);
            tb.add("POIID", Long.class);
            ds.createSchema(tb.buildFeatureType());
            //设置编码
//            Charset charset =  StandardCharsets.UTF_8;
            Charset charset = Charset.forName("gbk");
            ds.setCharset(charset);
            //设置Writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);

            for (int i = 0, len = features.size(); i < len; i++) {
                String strFeature = features.get(i).toString();
                Reader reader = new StringReader(strFeature);
                SimpleFeature feature = writer.next();
                feature.setAttribute("the_geom", gjson.readMultiPolygon(reader));
                feature.setAttribute("POIID", i);
                writer.write();
            }
            writer.close();
            ds.dispose();
            map.put("status", "success");
            map.put("message", shpPath);
        } catch (Exception e) {
            map.put("status", "failure");
            map.put("message", e.getMessage());
            e.printStackTrace();
        }
        return map;
    }

    /**
     * shp转换为Geojson
     *
     * @param shpPath
     * @return
     */
    public static Map<String, String> shape2Geojson(String shpPath, String jsonPath) {
        Map<String, String> map = new HashMap<>();

        FeatureJSON fjson = new FeatureJSON();

        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"type\": \"FeatureCollection\",\"features\": ");

            File file = new File(shpPath);
            ShapefileDataStore shpDataStore = null;

            shpDataStore = new ShapefileDataStore(file.toURL());
            //设置编码
//            Charset charset = StandardCharsets.UTF_8; // 10.2以上版本
            Charset charset = Charset.forName("gbk");
            shpDataStore.setCharset(charset);
            String typeName = shpDataStore.getTypeNames()[0];
            SimpleFeatureSource featureSource = null;
            featureSource = shpDataStore.getFeatureSource(typeName);
            SimpleFeatureCollection result = featureSource.getFeatures();
            SimpleFeatureIterator iterator = result.features();
            JSONArray array = new JSONArray();
            while (iterator.hasNext()) {
                SimpleFeature feature = iterator.next();
                StringWriter writer = new StringWriter();
                fjson.writeFeature(feature, writer);
                JSONObject json = JSONObject.parseObject(writer.toString());
                array.add(json);
            }
            iterator.close();
            sb.append(array.toString());
            sb.append("}");

            //写入文件
            FileUtils.writeJsonFile(jsonPath, sb.toString());

            map.put("status", "success");
            map.put("message", sb.toString());
        } catch (Exception e) {
            map.put("status", "failure");
            map.put("message", e.getMessage());
            e.printStackTrace();

        }
        return map;
    }
}
