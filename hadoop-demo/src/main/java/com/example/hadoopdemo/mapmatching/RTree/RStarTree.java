package com.example.hadoopdemo.mapmatching.RTree;

import com.example.hadoopdemo.mapmatching.bean.Geometry;

import java.util.*;

/**
 * Created by AJQK on 2019/12/30.
 */
public class RStarTree<T extends Geometry> {
    private TreeNode<T> root;
    private int height;//树的总高度
    private static final int M = 50;// 完全参考论文的实验环境了 其实论文是有其他考虑的
    private static final int m = 20;// M的40%，据论文而言效率更高
    private static final int p = 32;
    private static final int pReInsert = 15;// M的30%
//    private static final int M = 4;// 完全参考论文的实验环境了 其实论文是有其他考虑的
//    private static final int m = 2;// M的40%，据论文而言效率更高
//    private static final int p = 4;
//    private static final int pReInsert = 1;// M的30%

    public RStarTree() {
        root = new TreeNode<>();
        overflowRecord.clear();
        height = 1;
    }

    public TreeNode<T> getRoot() {
        return root;
    }

    // 1.选择插入点
    private TreeNode<T> chooseSubtree(TreeNode<T> newData, int level){
        TreeNode<T> result = root;
        int curLevel = 1;
        while(result!=null && !result.isLeaf() && curLevel!=level){
            TreeNode<T> next = null;
            double areaEnlargement = Double.MAX_VALUE;
            double area = Double.MAX_VALUE;
            double overlapEnlargement = Double.MAX_VALUE;
            List<ModifyNode> nodes = modifyHelper(newData, result.getChildren());
            for(ModifyNode modifyNode:nodes){
                TreeNode<T> child = modifyNode.getTreeNode();
                Rectangle rectangle = modifyNode.getRectangle();
                double _area = modifyNode.getArea();
                double _areaEnlargement = modifyNode.getAreaEnlargement();
                if(child.isLeaf()){
                    double _newOverlap = TreeHelper.getOverlap(rectangle,child.getRectangle(), result.getChildren());
                    double _originOverlap = TreeHelper.getOverlap(child.getRectangle(),child.getRectangle(), result.getChildren());
                    double _overlapEnlargement = _newOverlap - _originOverlap;
                    if(_overlapEnlargement<overlapEnlargement){
                        overlapEnlargement = _overlapEnlargement;
                        areaEnlargement = _areaEnlargement;
                        area = _area;
                        next = child;
                    }else if(_overlapEnlargement==overlapEnlargement && _areaEnlargement<areaEnlargement){
                        overlapEnlargement = _overlapEnlargement;
                        areaEnlargement = _areaEnlargement;
                        area = _area;
                        next = child;
                    }else if(_overlapEnlargement==overlapEnlargement && _areaEnlargement == areaEnlargement && _area < area){
                        overlapEnlargement = _overlapEnlargement;
                        areaEnlargement = _areaEnlargement;
                        area = _area;
                        next = child;
                    }
                }else{
                    if(_areaEnlargement<areaEnlargement){
                        areaEnlargement = _areaEnlargement;
                        area = _area;
                        next = child;
                    }else if(_areaEnlargement == areaEnlargement && _area < area){
                        areaEnlargement = _areaEnlargement;
                        area = _area;
                        next = child;
                    }
                }
            }
            result = next;
//            if(curLevel==level) break;
        }
        return result;
    }
    private class ModifyNode{
        private TreeNode<T> treeNode;
        private Rectangle rectangle;
        private double area;
        private double areaEnlargement;

        public TreeNode<T> getTreeNode() {
            return treeNode;
        }

        public void setTreeNode(TreeNode<T> treeNode) {
            this.treeNode = treeNode;
        }

        public Rectangle getRectangle() {
            return rectangle;
        }

        public void setRectangle(Rectangle rectangle) {
            this.rectangle = rectangle;
        }

        public double getArea() {
            return area;
        }

        public void setArea(double area) {
            this.area = area;
        }

        public double getAreaEnlargement() {
            return areaEnlargement;
        }

        public void setAreaEnlargement(double areaEnlargement) {
            this.areaEnlargement = areaEnlargement;
        }
    }
    private List<ModifyNode> modifyHelper(TreeNode<T> newData,List<TreeNode<T>> children){
        List<ModifyNode> result = new LinkedList<>();
        for(TreeNode<T> child: children){
            ModifyNode node = new ModifyNode();
            node.setTreeNode(child);
            node.setRectangle(TreeHelper.getNewRectangle(child.getRectangle(),newData.getRectangle()));
            node.setArea(TreeHelper.getArea(node.getRectangle()));
            node.setAreaEnlargement(node.getArea()-TreeHelper.getArea(child.getRectangle()));
            result.add(node);
        }
        if(result.size()<p) return result;
        result.sort((o1, o2) -> Double.compare(o2.getAreaEnlargement(), o1.getAreaEnlargement()));
        return new LinkedList<>(result.subList(0, p));
    }
    // 2.分割算法 entities为M+1个
    private SplitNode split(List<TreeNode<T>> entities){
        double xScore=0, yScore =0;
        entities.sort((Comparator.comparingDouble(o -> o.getRectangle().getXmin())));
        List<TreeNode<T>> x1 = new LinkedList<>(entities);
        xScore+= getSplitValue(entities);
        entities.sort((Comparator.comparingDouble(o -> o.getRectangle().getXmax())));
        List<TreeNode<T>> x2 = new LinkedList<>(entities);
        xScore+= getSplitValue(entities);
        entities.sort((Comparator.comparingDouble(o -> o.getRectangle().getYmin())));
        List<TreeNode<T>> y1 = new LinkedList<>(entities);
        yScore+= getSplitValue(entities);
        entities.sort((Comparator.comparingDouble(o -> o.getRectangle().getYmax())));
        List<TreeNode<T>> y2 = new LinkedList<>(entities);
        yScore+= getSplitValue(entities);
//        int axis = xScore<yScore ? 0 : 1;
        return xScore<yScore?distribute(x1,x2):distribute(y1,y2);

    }
    private double getSplitValue(List<TreeNode<T>> entities){
        double result = 0;
        for(int i = m;i<= M - m;i++){
            List<TreeNode<T>> left = new LinkedList<>(entities.subList(0, i));
            List<TreeNode<T>> right = new LinkedList<>(entities.subList(i,entities.size()));
            Rectangle rec1 = TreeHelper.getNewRectangle(left);
            Rectangle rec2 = TreeHelper.getNewRectangle(right);
            result += TreeHelper.getMargin(rec1);
            result += TreeHelper.getMargin(rec2);
        }
        return result;
    }
    private class SplitNode{
        private List<TreeNode<T>> left;
        private List<TreeNode<T>> right;
        private double area = 0;
        private double overlap = 0;

        public SplitNode() {
        }

        public SplitNode(List<TreeNode<T>> left, List<TreeNode<T>> right, double area, double overlap) {
            this.left = left;
            this.right = right;
            this.area = area;
            this.overlap = overlap;
        }

        public List<TreeNode<T>> getLeft() {
            return left;
        }

        public void setLeft(List<TreeNode<T>> left) {
            this.left = left;
        }

        public List<TreeNode<T>> getRight() {
            return right;
        }

        public void setRight(List<TreeNode<T>> right) {
            this.right = right;
        }

        public double getArea() {
            return area;
        }

        public void setArea(double area) {
            this.area = area;
        }

        public double getOverlap() {
            return overlap;
        }

        public void setOverlap(double overlap) {
            this.overlap = overlap;
        }
    }
    private SplitNode distribute(List<TreeNode<T>> entities1, List<TreeNode<T>> entities2){
        SplitNode tmp1, tmp2;
//        switch (axis){
//            case 0:
//                entities.sort(((o1, o2) -> o1.getRectangle().getXmin()<o2.getRectangle().getXmin()?-1:1));
//                tmp1 = getDistribution(entities);
//                entities.sort(((o1, o2) -> o1.getRectangle().getXmax()<o2.getRectangle().getXmax()?-1:1));
//                tmp2 = getDistribution(entities);
//                break;
//            case 1:
//                entities.sort(((o1, o2) -> o1.getRectangle().getYmin()<o2.getRectangle().getYmin()?-1:1));
//                tmp1 = getDistribution(entities);
//                entities.sort(((o1, o2) -> o1.getRectangle().getYmax()<o2.getRectangle().getYmax()?-1:1));
//                tmp2 = getDistribution(entities);
//                break;
//            default:
//                return null;
//        }
        tmp1 = getDistribution(entities1);
        tmp2 = getDistribution(entities2);
        if(tmp1.getOverlap()<tmp2.getOverlap() ||(tmp1.getOverlap()==tmp2.getOverlap() && tmp1.getArea()<tmp2.getArea()))
            return tmp1;
        else
            return tmp2;
    }
    private SplitNode getDistribution(List<TreeNode<T>> entities){
        SplitNode result = null;
        for(int i = m ;i<= M - m;i++){
            List<TreeNode<T>> left = new LinkedList<>(entities.subList(0, i));
            List<TreeNode<T>> right = new LinkedList<>(entities.subList(i,entities.size()));
            Rectangle rec1 = TreeHelper.getNewRectangle(left);
            Rectangle rec2 = TreeHelper.getNewRectangle(right);
            double area = TreeHelper.getArea(rec1) + TreeHelper.getArea(rec2);
            double overlap = TreeHelper.getOverlap(rec1, rec2);
            if(result==null)
                result = new SplitNode(left,right,area,overlap);
            else if(overlap<result.getOverlap()){
                result.setLeft(left);
                result.setRight(right);
                result.setArea(area);
                result.setOverlap(overlap);
            }else if(overlap == result.getOverlap() && area<result.getArea()){
                result.setLeft(left);
                result.setRight(right);
                result.setArea(area);
                result.setOverlap(overlap);
            }
        }
        return result;
    }
    // 3. 插入
    private Set<Integer> overflowRecord = new HashSet<>();
    public void insertData(TreeNode<T> newData){
        insert(height,newData);
    }
    private void insert(int level, TreeNode<T> newData){
        TreeNode<T> node = chooseSubtree(newData, level);
        newData.setParent(node);
        node.getChildren().add(newData);
//        if(node.getChildren().size()<M){
//            newData.setParent(node);
//            node.getChildren().add(newData);
//        }else{
//            List<TreeNode<T>> nodes = new LinkedList<>(node.getChildren());
//            nodes.add(newData);
//            overflowTreatment(level, node, nodes);
//        }
        if(node.getChildren().size()>M)
            overflowTreatment(level, node);
        else
            updateRectanglePropagate(node);
    }
    private void overflowTreatment(int level, TreeNode<T> node){
        boolean isOverflow = overflowRecord.contains(level);
        overflowRecord.add(level);
        if(level!=1 && !isOverflow){
            reInsert(level,node);
        }else{
            SplitNode splitNode = split(node.getChildren());
            TreeNode<T> node1 = new TreeNode<>();
            node1.setParent(node.getParent());
            node1.setChildren(splitNode.getLeft());
            node1.updateChildren();
            TreeNode<T> node2 = new TreeNode<>();
            node2.setParent(node.getParent());
            node2.setChildren(splitNode.getRight());
            node2.updateChildren();
            if(node.getParent()!=null){
                List<TreeNode<T>> brothers = node.getParent().getChildren();
                brothers.remove(node);
                brothers.add(node1);
                brothers.add(node2);
                if(brothers.size() > M)
                    overflowTreatment(level - 1, node.getParent());
                else
                    updateRectanglePropagate(node.getParent());// 更新范围应该是对的 今天脑子比较懵 明天检查一下
            }else{
                TreeNode<T> newRoot = new TreeNode<>();
                newRoot.addChild(node1);
                newRoot.addChild(node2);
                newRoot.updateChildren();
                updateRectangle(newRoot);
                root = newRoot;
                height++;
            }
        }
    }
    private void updateRectanglePropagate(TreeNode<T> node){
        updateRectangle(node);
        if(node.getParent()!=null)
            updateRectanglePropagate(node.getParent());
    }
    private void updateRectangle(TreeNode<T> node){
        node.setRectangle(TreeHelper.getNewRectangle(node.getChildren()));
    }
    // entities为M+1个
    private void reInsert(int level, TreeNode<T> node){
        Centroid centroid = TreeHelper.getCentroid(node.getRectangle());
        List<TreeNode<T>> children = node.getChildren();
        children.sort((o1, o2) -> {
            Centroid p1 = TreeHelper.getCentroid(o1.getRectangle());
            Centroid p2 = TreeHelper.getCentroid(o2.getRectangle());
            double d1 = TreeHelper.getDistance(p1,centroid);
            double d2 = TreeHelper.getDistance(p2,centroid);
            return d1 == d2 ? 0 : (d1<d2?1:-1);//降序
        });
        List<TreeNode<T>> front = new LinkedList<>(children.subList(0, pReInsert));
        List<TreeNode<T>> back =  new LinkedList<>(children.subList(pReInsert, children.size()));
        node.setChildren(back);
        updateRectanglePropagate(node);//最好还是递归 不然不对20200110!
        for(TreeNode<T> n: front){
            insert(level,n);//能否找到空的好的插入点 走的是插入的逻辑 无需多虑
        }
    }
    // 4.搜索 这个就自己写吧
    public List<T> searchData(Rectangle rectangle){
        List<T> result = new LinkedList<>();
        addAllBySearch(root, rectangle, result);
        return result;
    }
    private void addAllWithoutSearch(TreeNode<T> node, List<T> result){
        if(node.getChildren().size()==0)
            result.add(node.getData());
        else{
            for(TreeNode<T> child:node.getChildren()){
                addAllWithoutSearch(child, result);
            }
        }
    }
    private void addAllBySearch(TreeNode<T> node, Rectangle extent, List<T> result){
        Rectangle rectangle = node.getRectangle();
        if(TreeHelper.contains(extent, rectangle)){
            addAllWithoutSearch(node, result);
        }else if(TreeHelper.intersect(extent, rectangle)){
            if(node.getChildren().size()==0) {
                if (node.getData().intersect(extent))
                    result.add(node.getData());
            }else{
                for(TreeNode<T> child:node.getChildren()){
                    addAllBySearch(child, extent, result);
                }
            }
        }
    }
}
