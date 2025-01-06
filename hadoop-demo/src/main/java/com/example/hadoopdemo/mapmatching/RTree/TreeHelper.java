package com.example.hadoopdemo.mapmatching.RTree;

import com.example.hadoopdemo.mapmatching.bean.Geometry;

import java.util.List;

/**
 * Created by AJQK on 2019/12/31.
 */
public class TreeHelper {
    public static double getOverlap(Rectangle rec1, Rectangle rec2){
        double xmin = Math.max(rec1.getXmin(), rec2.getXmin());
        double xmax = Math.min(rec1.getXmax(), rec2.getXmax());
        double ymin = Math.max(rec1.getYmin(), rec2.getYmin());
        double ymax = Math.min(rec1.getYmax(), rec2.getYmax());
        if(xmin>xmax || ymin>ymax)
            return 0;
        return (xmax-xmin)*(ymax-ymin);
    }
    public static Rectangle getNewRectangle(Rectangle old, Rectangle item){
        double xmin = Math.min(old.getXmin(), item.getXmin());
        double xmax = Math.max(old.getXmax(), item.getXmax());
        double ymin = Math.min(old.getYmin(), item.getYmin());
        double ymax = Math.max(old.getYmax(), item.getYmax());
        return new Rectangle(xmin, xmax, ymin, ymax);
    }
    public static double getArea(Rectangle rec){
        return (rec.getXmax()-rec.getXmin())*(rec.getYmax()-rec.getYmin());
    }
    public static <T extends Geometry> double getOverlap(Rectangle rec1, Rectangle except, List<TreeNode<T>> others){
        double overlap = 0;
        for(TreeNode<T> rec: others){
            if(rec.getRectangle() == except) continue;
            overlap += getOverlap(rec1, rec.getRectangle());
        }
        return overlap;
    }
    public static <T extends Geometry> Rectangle getNewRectangle(List<TreeNode<T>> nodes){
        double xmin=Double.MAX_VALUE, xmax=Double.MIN_VALUE, ymin=Double.MAX_VALUE, ymax=Double.MIN_VALUE;
        for(TreeNode<T> node:nodes){
            xmin = Math.min(xmin, node.getRectangle().getXmin());
            xmax = Math.max(xmax, node.getRectangle().getXmax());
            ymin = Math.min(ymin, node.getRectangle().getYmin());
            ymax = Math.max(ymax, node.getRectangle().getYmax());
        }
        return new Rectangle(xmin, xmax, ymin, ymax);
    }

    /**
     * 有人翻译margin是周长，暂时先用周长看看情况
     * @param rectangle 矩形
     * @return 周长
     */
    public static double getMargin(Rectangle rectangle){
        return (rectangle.getXmax()-rectangle.getXmin())*2+(rectangle.getYmax()-rectangle.getYmin())*2;
    }
    public static Centroid getCentroid(Rectangle rectangle){
        return new Centroid((rectangle.getXmin()+rectangle.getXmax())/2,(rectangle.getYmin()+rectangle.getYmax())/2);
    }
    public static double getDistance(Centroid point1, Centroid point2){
        return Math.sqrt(Math.pow(point1.getX()-point2.getX(),2)+Math.pow(point1.getY()-point2.getY(),2));
    }
    public static boolean contains(Rectangle rec1, Rectangle rec2){
        return rec1.getXmin()<=rec2.getXmin() && rec1.getYmin()<=rec2.getYmin()
                && rec1.getXmax()>=rec2.getXmax() && rec1.getYmax()>=rec2.getYmax();
    }
    public static boolean intersect(Rectangle rec1, Rectangle rec2){
        double xmin = Math.max(rec1.getXmin(), rec2.getXmin());
        double xmax = Math.min(rec1.getXmax(), rec2.getXmax());
        double ymin = Math.max(rec1.getYmin(), rec2.getYmin());
        double ymax = Math.min(rec1.getYmax(), rec2.getYmax());
        return !(xmin > xmax || ymin > ymax);
    }
    public static boolean contains(double x, double y, Rectangle rec){
        return x<=rec.getXmax() && x>=rec.getXmin() &&
                y<=rec.getYmax() && y>=rec.getYmin();
    }
    public static boolean intersect(double x11, double y11, double x12, double y12,
                                    double x21, double y21, double x22, double y22){
        double delta = determinant(x12-x11,x22-x21, y22-y21, y12-y11);
        // 20200630 注释，对于经纬度而言，这个范围有点高了，造成了附近的线没有获取到的问题
//        if ( delta<=(1e-6) && delta>=-(1e-6) )  // delta=0，表示两线段重合或平行
//        {
//            return false;
//        }
        double namenda = determinant(x22-x21,x11-x21, y11-y21, y22-y21) / delta;
        if ( namenda>1 || namenda<0 )
        {
            return false;
        }
        double miu = determinant(x12-x11, x11-x21,  y11-y21, y12-y11) / delta;
        return !(miu > 1 || miu < 0);
    }
    public static double determinant(double v1, double v2, double v3, double v4)  // 行列式
    {
        return (v1 * v3 - v2 * v4);
    }
}
