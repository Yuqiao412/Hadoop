package com.example.hadoopdemo.mapmatching.match;

import com.example.hadoopdemo.mapmatching.RTree.Rectangle;
import com.example.hadoopdemo.mapmatching.bean.Line;
import com.example.hadoopdemo.mapmatching.bean.Pair;
import com.example.hadoopdemo.mapmatching.bean.Point;
import com.example.hadoopdemo.mapmatching.data.RoadData;
import com.example.hadoopdemo.mapmatching.utils.Transform;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class PointMatching {
	// 进行道路匹配
	// returnMax 当两条待定路线得分相近时 是否一定返回一个点
	private static final float envelope = 0.00075F;
	private static final float score = 0.75F;
//	private static final float scoreThreshold = 0.005F;
	private static final float scoreThreshold = 0.015F;
	private static final float distanceThreshold = 5;// 大约5m
	private static final int angleThreshold = 5;
	public static Point Matching(Point point, boolean returnMax) {
		List<Double> tmp_distance=new ArrayList<>();//保存所有的距离
		List<Float> tmp_angle=new ArrayList<>();//保存所有的角度差!
		List<Point> tmp_point=new ArrayList<>();//保存所有的垂足
		List<Double> tmp_r = new ArrayList<>();
		List<Line> lines = RoadData.getLinesByExtent(point.getX()-envelope,point.getX()+envelope,point.getY()-envelope,point.getY()+envelope);
		if(lines.size()==0)
			return null;
		List<Line> selectedLines = new ArrayList<>();
		for (Line line : lines) {
			int direction = line.getDirection();//0未分类，1双向通行，2正向，3反向，4禁行
			double x1 = line.getStart().getX();
			double y1 = line.getStart().getY();
			double x2 = line.getEnd().getX();
			double y2 = line.getEnd().getY();
			float lineAngle = Common.CalculateAngle(x1, y1, x2, y2);
			float angle = Common.CalculateAngleDeviationByDirection(direction, point.getAngle(), lineAngle);
			if(angle>60)
				continue;
			//根据路的方向 计算车行驶方向与路的夹角

			double r = Common.CalculateR(point, x1, y1, x2, y2);
			Point pedal = CalculatePoint(point, x1, y1, x2, y2,r);
			double dis = Transform.getEuclideanDistance(point, pedal);
			if(dis<40 || (r>=-0.1 && r<=1.1)){
				tmp_angle.add(angle);
				tmp_r.add(r);
				tmp_point.add(pedal);
				tmp_distance.add(dis);
				selectedLines.add(line);
			}
		}
		int index = GetBestIndex(tmp_angle, tmp_distance,tmp_r, returnMax);
		if(index==-1)
			return null;//20190514
		Point result = tmp_point.get(index);
		result.setLineID(selectedLines.get(index).getLineid());
		result.setRoadID(selectedLines.get(index).getRoadid());
		result.setAngle(point.getAngle());
		result.setTime(point.getTime());
		result.setVelocity(point.getVelocity());
		result.setStatus(point.getStatus());
		return result;
	}
	//20200302
	private static int GetBestIndex(List<Float> angle, List<Double> distance, List<Double> r, boolean returnMax){
		List<Double> nor_angle=GetNormalList(angle,0,90);
		List<Double> nor_distance=GetNormalList(distance,0,100);
		double weight_angle = 0.7;
		double weight_distance = 0.3;
		List<Pair<Integer, Double>> scores = new LinkedList<>();

		for(int i=0;i<angle.size();i++){
			double score = nor_angle.get(i)*weight_angle+nor_distance.get(i)*weight_distance;
			if(score>0)
				scores.add(new Pair<>(i,score));
		}
		if(scores.size()==0)
			return -1;
		scores.sort((o1, o2) -> {
			if(o2.getB().equals(o1.getB())){
				// 角度差或者距离差最小的放前面
				if(nor_angle.get(o2.getA())<nor_angle.get(o1.getA()))
					return 1;
				else if(Objects.equals(nor_angle.get(o2.getA()), nor_angle.get(o1.getA())) && nor_distance.get(o2.getA())<nor_distance.get(o1.getA()))
					return Double.compare(nor_distance.get(o1.getA()), nor_distance.get(o2.getA()));
				else
					return -1;
			}else{
				return Double.compare(o2.getB(),o1.getB());
			}
		});

		double maxScore = scores.get(0).getB();
		int maxScoreIndex = scores.get(0).getA();
		// 20200410 为了解决前两个分数相近，但最高的匹配到r不在0-1范围内的线段上导致的路段匹配错误问题
		if(scores.size()==1) {
			if(returnMax || maxScore>=score)
				return maxScoreIndex;
			else
				return -1;
		}

		double secondScore = scores.get(1).getB();
		int secondIndex = scores.get(1).getA();
		double d = Math.abs(distance.get(maxScoreIndex)-distance.get(secondIndex));
		double a = Math.abs(angle.get(maxScoreIndex)-distance.get(secondIndex));
		if(!returnMax && maxScore - secondScore<scoreThreshold && d<distanceThreshold && a<angleThreshold){
			return -1;
		}
		if((r.get(maxScoreIndex)<0||r.get(maxScoreIndex)>1) && r.get(secondIndex)>=0 && r.get(secondIndex)<=1)
			return secondIndex;
		return maxScoreIndex;
	}

//	private static<T extends Number> List<Double> GetNormalList(List<? extends T> list){
//		Double min=Double.MAX_VALUE;
//		Double max=Double.MIN_VALUE;
//		for(int i = 0;i<list.size();i++){
//			if(list.get(i).doubleValue()>max)
//				max=list.get(i).doubleValue();
//			if(list.get(i).doubleValue()<min)
//				min=list.get(i).doubleValue();
//		}
//		List<Double> norList=new ArrayList<>();
//		if(min == max){
//			for(int i=0;i<list.size();i++){
//				norList.add(1d);
//			}
//		}
//		else{
//			for(int i=0;i<list.size();i++){
//				double nor = (max-list.get(i).doubleValue())/(max-min);
//				norList.add(nor);
//			}
//		}
//		
//		return norList;
//	}
	private static<T extends Number> List<Double> GetNormalList(List<? extends T> list, double min , double max){
		List<Double> norList=new ArrayList<>();
		if(min == max){
			for(int i=0;i<list.size();i++){
				norList.add(1d);
			}
		}
		else{
			for (T aList : list) {
				double nor = (max - aList.doubleValue()) / (max - min);
				norList.add(nor);
			}
		}
		
		return norList;
	}

	public static Point CalculatePoint(Point current, Point start, Point end, double r){
		return CalculatePoint(current,start.getX(),start.getY(),end.getX(),end.getY(),r);
	}

	private static Point CalculatePoint(Point current,double x1,double y1,double x2,double y2,double r){
		Point result;
		if (r < 0)
			result=new Point(x1, y1);
		else if (r > 1)
			result=new Point(x2, y2);
		else{
			if(x1 == x2){
				result=new Point(x1, current.getY());
			}
			else{
				double xr=x1 + r * (x2 - x1);
				double yr=y1 + r * (y2 - y1);
				result=new Point(xr, yr);
			}
		}
		return result;
	}
/* 使用postgis
	private static List<Map<String, Object>> getLineList(Point point) throws Exception{
		int gridNum = Common.getGridNum(point.getX(), point.getY());
		int grid_x=Common.getGridX(point.getX());
		int grid_y=Common.getGridY(point.getY());
		if(grid_x>=4250||grid_y>=3370)
			return new ArrayList<Map<String,Object>>();
		int[][] constant={{gridNum-Common.width-1,gridNum-Common.width,gridNum-Common.width+1},
				{gridNum-1,gridNum,gridNum+1},{gridNum+Common.width-1,gridNum+Common.width,gridNum+Common.width+1}};
		
		int minx,maxx,miny,maxy;
		if(grid_y==0){
			miny=1;
			maxy=2;
		}
		else if(grid_y==Common.height-1){
			miny=0;
			maxy=1;
		}	
		else{
			miny=0;
			maxy=2;
		}
		if(grid_x==0){
			minx=1;
			maxx=2;
		}
		else if(grid_x==Common.width-1){
			minx=0;
			maxx=1;
		}
		else{
			minx=0;
			maxx=2;
		}
		int gridnums = (maxx-minx+1)*(maxy-miny+1);
		Object[] grids=new Object[gridnums];
		int count=0;
		String whereClause = "";
		for(int i = minx;i <= maxx; i++){
			for(int j = miny;j <= maxy; j++){
				grids[count]=constant[i][j];
				if(count!=0){
					whereClause+=",";
				}
				whereClause+="?";
				count++;
			}
		}
		if(whereClause.equals(""))
			return new ArrayList<Map<String,Object>>();
		String sql="select lineids from grids where gridid in ("+whereClause+")";
		return CRUD.read(sql, grids);
	}
	private static List<Map<String, Object>> getLineListOnceagain(Point point) throws Exception{
		int gridNum = Common.getGridNum(point.getX(), point.getY());
		int grid_x=Common.getGridX(point.getX());
		int grid_y=Common.getGridY(point.getY());
		if(grid_x>=Common.width||grid_y>=Common.height)
			return new ArrayList<Map<String,Object>>();
		int[][] constant={{gridNum-Common.width*2-2,gridNum-Common.width*2-1,gridNum-Common.width*2,gridNum-Common.width*2+1,gridNum-Common.width*2+2},
				{gridNum-Common.width-2,gridNum-Common.width+2},
				{gridNum-2,gridNum+2},
				{gridNum+Common.width-2,gridNum+Common.width+2},
				{gridNum+Common.width*2-2,gridNum+Common.width*2-1,gridNum+Common.width*2,gridNum+Common.width*2+1,gridNum+Common.width*2+2}};
		List<Integer> gnums=new ArrayList<>();
		String whereClause = "";
		int count=0;
		for(int i =0;i<5;i++){
			if(grid_x-(2-i)<0 ||grid_x-(2-i)>=Common.width-1)
				continue;
			if(i==0 || i==4){
				for(int j=0;j<5;j++){
					if(grid_y-(2-j)<0||grid_y-(2-j)>=Common.height-1)
						continue;
					else{
						gnums.add(constant[i][j]);
						if(count!=0){
							whereClause+=",";
						}
						whereClause+="?";
						count++;
					}
						
				}
			}else{
				if(grid_y-2<0 || grid_y-2>=Common.height-1)
					continue;
				else{
					gnums.add(constant[i][0]);
					if(count!=0){
						whereClause+=",";
					}
					whereClause+="?";
					count++;
				}
				if(grid_y+2<0 || grid_y+2>=Common.height-1)
					continue;
				else{
					gnums.add(constant[i][1]);
					if(count!=0){
						whereClause+=",";
					}
					whereClause+="?";
					count++;
				}
			}
			
		}
		if(whereClause.equals(""))
			return new ArrayList<Map<String,Object>>();
		Object[] grids=gnums.toArray(new Object[gnums.size()]);
		String sql="select lineids from grids where gridid in ("+whereClause+")";
		return CRUD.read(sql, grids);
	}
*/
}
