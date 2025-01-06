package com.example.hadoopdemo.mapmatching.mapreduce;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.utils.Transform;

public class VehiclePoint {
	private Point mercator;
	private int weight;// 空重状态
	private long date;
	private String[] values;
	private int statue = -1;
	private float v ;
//	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	private static DecimalFormat decimalFormat = new DecimalFormat("000000");
	public VehiclePoint(String value){
		value=value.endsWith(";")?value.substring(0,value.length()-1):value;
		values = value.split("[,;]");
		Point lonlat=new Point(Double.parseDouble(values[4]),Double.parseDouble(values[5]));
		weight=Integer.parseInt(values[8])==1?1:0;
		mercator= Transform.lonLat2Mercator(lonlat);
		v=Float.parseFloat(values[6]);

		try {
			values[1] = decimalFormat.format(Integer.parseInt(values[1]));
			date = sdf.parse(values[0]+values[1]).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public Point getMercator(){
		return mercator;
	}
	public long getDate(){
		return date;
	}
	public int getWeight(){
		return weight;
	}
//	public String getValue(){
//		return value+","+statue;
//	}
	public String getValue(int i){
//		Point p = OffsetCorrection.Transform(new Point(Double.parseDouble(values[4]),Double.parseDouble(values[5]))); // 需要坐标转换用这个
		Point p = new Point(Double.parseDouble(values[4]),Double.parseDouble(values[5]));
		values[3] = values[3]+"_"+i;
		values[4] = String.valueOf(p.getX());
		values[5] = String.valueOf(p.getY());
		StringBuilder val= new StringBuilder();
		for(int j=0;j<values.length;j++){
			val.append(values[j]);
			if(j!=values.length-1){
				val.append(",");
			}
		}
		return val+","+statue;
		
	}
	public void setStatue(int s){
		statue=s;
	}
	public int getStatue(){
		return statue;
	}
	public int check(VehiclePoint p){
		int status;
		double distance = Math.sqrt(Math.pow((mercator.getX()-p.getMercator().getX()), 2)+Math.pow((mercator.getY()-p.getMercator().getY()), 2));
		long time = date-p.getDate();
		if(p.getDate()-date>1000*30*60){
			status = 5; //超过半个小时的 直接打断 太浪费时间了,之前是看是否超过4000m 120km/h * 2min
		}else if(weight != p.getWeight()){
			status= weight==1?1:0; // 0 get off 1 get on
		}else if((distance/time*60*60>120 && time!=0)||(time==0 && distance>50)){
			status = 3;//wrong
		}else if(distance<10){
			status = 4;//stay
		}else{
			status = 2;//on the way
		}
		return status;
	}
	public int check2(VehiclePoint p){
		int status;
		double distance = Math.sqrt(Math.pow((mercator.getX()-p.getMercator().getX()), 2)+Math.pow((mercator.getY()-p.getMercator().getY()), 2));
		long time = date-p.getDate();
		if(time>1000*30*60){
			status = 5; //超过半个小时的 直接打断 太浪费时间了,之前是看是否超过4000m 120km/h * 2min
		}else if((distance/time*60*60>120 && time!=0) || (time==0 && distance>50)){
			status = 3;//wrong
		}else if(v==0 || distance<10){
			status = 4;//stay
		}else{
			status = 2;//on the way
		}
		return status;
	}
}
