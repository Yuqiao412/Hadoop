package com.example.hadoopdemo.mapmatching.mapreduce;

import com.example.hadoopdemo.mapmatching.bean.Point;

public class OffsetCorrection {
	private static double DEF_PI = 3.14159265358979324;
	// 2*PI
//	private static double DEF_2PI = 6.28318530712;
//	// PI/180.0
//	private static double DEF_PI180 = 0.01745329252;
//	// radius of earth wgs84
//	private static double DEF_R = 6370693.5;
	//
	// KRASOVSKY 1940
	//
	// a = 6378245.0, 1/f = 298.3
	// b = a * (1 - f)
	// ee = (a^2 - b^2) / a^2;
	private static double DEF_R_KRASOVSKY = 6378245.0;
	private static double DEF_EE_KRASOVSKY = 0.00669342162296594323;

	private static double transform_lat(double x, double y)
	{
		double ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * Math.sqrt(Math.abs(x));
		ret += (20.0 * Math.sin(6.0 * x * DEF_PI) + 20.0 * Math.sin(2.0 * x * DEF_PI)) * 2.0 / 3.0;
		ret += (20.0 * Math.sin(y * DEF_PI) + 40.0 * Math.sin(y / 3.0 * DEF_PI)) * 2.0 / 3.0;
		ret += (160.0 * Math.sin(y / 12.0 * DEF_PI) + 320 * Math.sin(y * DEF_PI / 30.0)) * 2.0 / 3.0;
		return ret;
	}

	private static double transform_lon(double x, double y)
	{
		double ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * Math.sqrt(Math.abs(x));
		ret += (20.0 * Math.sin(6.0 * x * DEF_PI) + 20.0 * Math.sin(2.0 * x * DEF_PI)) * 2.0 / 3.0;
		ret += (20.0 * Math.sin(x * DEF_PI) + 40.0 * Math.sin(x / 3.0 * DEF_PI)) * 2.0 / 3.0;
		ret += (150.0 * Math.sin(x / 12.0 * DEF_PI) + 300.0 * Math.sin(x / 30.0 * DEF_PI)) * 2.0 / 3.0;
		return ret;
	}

	public static Point Transform(Point point)//double wgLat, double wgLon, double& mgLat, double& mgLon
	{
		
		double dLat = transform_lat(point.getX() - 105.0, point.getY() - 35.0);
		double dLon = transform_lon(point.getX() - 105.0, point.getY() - 35.0);
		double radLat = point.getY() / 180.0 * DEF_PI;
		double magic = Math.sin(radLat);
		magic = 1 - DEF_EE_KRASOVSKY * magic * magic;
		double sqrtMagic = Math.sqrt(magic);
		dLat = (dLat * 180.0) / ((DEF_R_KRASOVSKY * (1 - DEF_EE_KRASOVSKY)) / (magic * sqrtMagic) * DEF_PI);
		dLon = (dLon * 180.0) / (DEF_R_KRASOVSKY / sqrtMagic * Math.cos(radLat) * DEF_PI);
		return new Point(point.getX()+dLon, point.getY()+dLat);
	}
}
