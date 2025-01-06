package com.example.hadoopdemo.mapmatching.match;


import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.utils.Transform;

public class Common {

	public static float CalculateAngleDeviationByDirection(int direction, float angle, float lineAngle){
		float result;
		float tmp_angle = lineAngle-180<0?lineAngle+180:lineAngle-180;
		switch(direction){//0未分类，1双向通行，2正向，3反向，4禁行
		case 0:
		case 1:
			float a1 = Math.abs(angle-lineAngle);
			if(a1>180) a1=360-a1;
			float a2 = Math.abs(angle-tmp_angle);
			if(a2>180) a2=360-a2;
			result = Math.min(a1,a2);
			break;
		case 2:
			result = Math.abs(angle-lineAngle);	
			break;
		case 3:
			result = Math.abs(angle-tmp_angle);
			break;
			default:
				result=360;
				break;
		}
		return result>180?360-result:result;
	}

	public static Point GetShapePointByLineDirection(Point point1, Point point2, int direction,
													 boolean isNear, Point reference){
		switch (direction) {
		case 0:
		case 1:
			return isNear == Transform.getDistance(point1, reference) > Transform.getDistance(point2, reference) ?
					point2:point1;
		case 2:
			return point1;
		case 3:
			return point2;
		default:
			return null;
		}

	}
	// 用于获取离参考点最远/近的点
	public static Point GetEdgePoint(Point point1, Point point2, int direction,
													 boolean isStart, Point reference,boolean isNear){
		switch (direction) {
			case 0:
			case 1:
				return !isNear == Transform.getDistance(point1, reference) > Transform.getDistance(point2, reference) ?
						point1:point2;
			case 3:
				return !isNear == isStart?point2:point1;
			default:
				return !isNear == isStart?point1:point2;
		}

	}

	public static float CalculateAngleDeviationWithoutConsideringTwoWay(int direction, float angle, float lineAngle){
		//这里不考虑双向通行，主要是用于判断数据库中的点顺序和真实道路的方向时使用
		//这里把双向和未分类都当成正向处理
		float result;
		float tmp_angle = lineAngle-180<0?lineAngle+180:lineAngle-180;
		switch(direction){//0未分类，1双向通行，2正向，3反向，4禁行
		case 0:
		case 1:
		case 2:
			result = Math.abs(angle-lineAngle);
			break;
		case 3:
			result = Math.abs(angle-tmp_angle);
			break;
			default:
				result=360;
				break;
		}
		return result>180?360-result:result;
	}
	/*计算点线相关度，详情参考论文“浮动车快速道路匹配算法_耿小峰”*/
	public static double CalculateR(Point current,double x1,double y1,double x2,double y2){
		return ((current.getX() - x1) * (x2 - x1) + (current.getY() - y1) * (y2 - y1))
				/ ((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1));
	}
	public static double CalculateR(Point current,Point p1,Point p2){
		return CalculateR(current,p1.getX(),p1.getY(),p2.getX(),p2.getY());
	}
	public static Point getSegmentNearPoint(Point p1, Point p2, Point p){
		double r = Common.CalculateR(p, p1.getX(), p1.getY(), p2.getX(), p2.getY());
		Point result;
		if(r>=1){
			result = p2;
		}else if(r<=0){
			result = p1;
		}else{
			if(p1.getX() == p2.getX()){
				result=new Point(p1.getX(), p.getY());
			}
			else{
				double xr=p1.getX() + r * (p2.getX() - p1.getX());
				double yr=p1.getY() + r * (p2.getY() - p1.getY());
				result=new Point(xr, yr);
			}
		}
		return result;
	}
	public static float CalculateAngle(double x1,double y1,double x2,double y2){
		double slope;
		if (x2 == x1)
		{
			if (y2 >= y1)
			{
				slope = 0;
			}
			else
			{
				slope = Math.PI;
			}
		}
		else if (x2>x1)
		{
			slope = Math.PI / 2 - Math.atan((y2 - y1) / (x2 - x1));
		}
		else {
			slope = 3 * Math.PI / 2  - Math.atan((y2 - y1) / (x2 - x1));
		}
		return Math.round(slope * 180 / Math.PI);
	}
	public static float CalculateAngle(Point p1,Point p2){
		return CalculateAngle(p1.getX(),p1.getY(),p2.getX(),p2.getY());
	}
//	public static int getGridNum(double x, double y){
//		int grid_x = getGridX(x);
//		int grid_y = getGridY(y);
//		return grid_y * width + grid_x;//宽4250 高3370
//	}
//	public static int getGridNum(int grid_x, int grid_y){
//		return grid_y * width + grid_x;
//	}
//	public static int getGridX(double x){
//		return (int) ((x - originX) / step);
//	}
//	public static int getGridY(double y){
//		return (int) ((y - originY) / step);
//	}
}