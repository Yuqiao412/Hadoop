package com.example.hadoopdemo.mapmatching.utils;
/*路线还原算法的第一步，简化要处理的点*/

import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.match.Common;

import java.util.LinkedList;
import java.util.List;

public class Simplify {
	/*根据方向和角度，先把关键拐点给挑出来*/
	public static final float angleThreshold = 30;//超过多少度就算转点
	public static final float distanceMinThreshold = 0.001F;//大约100米吧
	public static final float distanceMaxThreshold = 0.02F;// 大约2000米
	public static final float distanceThreshold2 = 0.0005F;// 大约50米
	public static List<Point> getTurningPoint(List<Point> points){
		List<Point> result = new LinkedList<>();
		Point last = null;//上一个确定的点
		int lastIndex = -1;
		for(int i=0;i<points.size();i++){
			Point p =points.get(i);
			if(i==0 || i==points.size()-1 ||isTurningPoint(last, p, angleThreshold) ||(i<points.size()-1 && isTurningPoint(last,p,points.get(i+1)))){
				if(lastIndex != i-1)
					result.add(points.get(i-1));
				result.add(p);
				last = p;
				lastIndex = i;
			}
//			else if (Transform.getDistance(p, last)> 0.0045){
//				result.add(p);
//				last = p;
//				lastIndex = i;
//			}
		}
		return simplifyAgain(result);
//		return result;
	}
	//上述算法得到的折点往往存在过密的情况，如果有三个点以上过于密集的（0.0002） 就只保留两端的
	public static List<Point> simplifyAgain(List<Point> points){
		List<Point> result = new LinkedList<>();
		for(int i=0;i<points.size();i++){
			Point p =points.get(i);
			if(i==0 || i==points.size()-1){
				result.add(p);
			}else if (isTurningPoint(p,points.get(i-1),150) ||Transform.getDistance(p,points.get(i-1))>distanceThreshold2){//|| Transform.getDistance(p,points.get(i+1))>distanceThreshold2
				result.add(p);
			}
		}
		return result;
	}
	//以下用于所有点做map-matching
	public static List<Integer> getTurningPointIndex(List<Point> points){
		List<Integer> result = new LinkedList<>();
		Point last = null;//上一个确定的点
		int lastIndex = -1;
		for(int i=0;i<points.size();i++){
			Point p =points.get(i);
			if(i==0 || i==points.size()-1 ||isTurningPoint(last, p, angleThreshold) ||
					(i<points.size()-1 && isTurningPoint(last,p,points.get(i+1)))){
				if(lastIndex != i-1)
					result.add(i-1);
				result.add(i);
				last = p;
				lastIndex = i;
			}
		}
		return simplifyAgain(result,points);
	}
	public static List<Integer> simplifyAgain(List<Integer> indexs, List<Point> points){
		List<Integer> result = new LinkedList<>();
		for(int i=0;i<indexs.size();i++){
			Point p =points.get(indexs.get(i));
			if(i==0 || i==indexs.size()-1){
				result.add(indexs.get(i));
			}else if (isTurningPoint(p,points.get(indexs.get(i-1)),150) || Transform.getDistance(p,points.get(indexs.get(i-1)))>distanceThreshold2){// || Transform.getDistance(p,points.get(indexs.get(i+1)))>distanceThreshold2
				result.add(indexs.get(i));
			}
		}
		return result;
	}
	//暂时只用GPS的方向判断试试看 p1是上一个确定要进行还原的点
	private static boolean isTurningPoint(Point p1,Point p2, float threshold ){
		double angle = Math.abs(p2.getAngle()-p1.getAngle());
		return Math.min(angle,360-angle)>threshold;
	}
	//实际的线拐角 用的是实际的三连续的点
	private static boolean isTurningPoint(Point p1,Point p2,Point p3){
		//但是可能这几个点的距离很近,只要这个点在两边都比较近就干掉
		if((p1.getX()==p2.getX() && p1.getY()==p2.getY())||p3.getX()==p2.getX() && p3.getY()==p2.getY())
			return false;
		double d1 = Transform.getDistance(p1, p2);
		double d2 = Transform.getDistance(p2, p3);
//		20200310 不能太远了
		if(d1>=distanceMaxThreshold || d2>=distanceMaxThreshold)
			return true;
		if(d1<=distanceMinThreshold && d2<=distanceMinThreshold)
			return false;
		double angle1 = Common.CalculateAngle(p1.getX(), p1.getY(), p2.getX(), p2.getY());
		double angle2 = Common.CalculateAngle(p2.getX(), p2.getY(), p3.getX(), p3.getY());
		return Math.abs(angle2-angle1)>angleThreshold;
	}
}
