package com.example.hadoopdemo.mapmatching.mapreduce.parallel;

import com.example.hadoopdemo.mapmatching.mapreduce.MyWritable;
import java.util.Objects;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairOfInts implements WritableComparable<PairOfInts>  {
	private int first;
	private int second;

	public PairOfInts() {
		// 默认构造函数
	}

	public PairOfInts(int first, int second) {
		this.first = first;
		this.second = second;
	}

	// 序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}

	// 反序列化方法
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}

	@Override
	public int hashCode() {
		return Objects.hash(first, second);
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof PairOfInts) {
			PairOfInts o = (PairOfInts) obj;
			return this.first == o.first && this.second == o.second;
		}
		return false;
	}
	// 比较方法
	@Override
	public int compareTo(PairOfInts o) {
		int cmp = Integer.compare(first, o.first);
		if (cmp != 0) {
			return cmp;
		}
		return Integer.compare(second, o.second);
	}

	// 输出方法，便于调试
	@Override
	public String toString() {
		return first + "," + second;
	}

	// Getter和Setter方法（如果需要）
	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

}
