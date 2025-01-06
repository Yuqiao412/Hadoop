package com.example.hadoopdemo.mapmatching.mapreduce.parallel;

import ch.hsr.geohash.GeoHash;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CandidateMap extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("[,]");
        double lon = Double.parseDouble(values[4]);
        double lat = Double.parseDouble(values[5]);
        int geoHashLength = 7;
        String code = GeoHash.withCharacterPrecision(lat, lon, geoHashLength).toBase32();
        context.write(new Text(code), value);
    }
}
