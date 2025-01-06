package com.example.hadoopdemo.mapmatching.RTree;

import com.example.hadoopdemo.mapmatching.bean.Geometry;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by AJQK on 2019/12/30.
 */
public class TreeNode<T extends Geometry> {
    private TreeNode<T> parent = null;
    private List<TreeNode<T>> children = new LinkedList<>();
    private Rectangle rectangle;
    private T data;

    public TreeNode() {
    }

    public TreeNode(Rectangle rectangle, T data) {
        this.rectangle = rectangle;
        this.data = data;
    }

    public TreeNode<T> getParent() {
        return parent;
    }

    public void setParent(TreeNode<T> parent) {
        this.parent = parent;
    }

    public List<TreeNode<T>> getChildren() {
        return children;
    }

    public void setChildren(List<TreeNode<T>> children) {
        this.children.clear();
        this.children.addAll(children);
        this.rectangle = TreeHelper.getNewRectangle(children);
    }
    public void addChild(TreeNode<T> child){
        this.children.add(child);
    }

    public Rectangle getRectangle() {
        return rectangle;
    }

    public void setRectangle(Rectangle rectangle) {
        this.rectangle = rectangle;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
    public boolean isLeaf() {
        return children.size() == 0 || children.get(0).getChildren().size() == 0;
//        return children==null || children.size()==0;
    }
    public void updateChildren(){
        for(TreeNode<T> child: children){
            child.setParent(this);
        }
    }
}
