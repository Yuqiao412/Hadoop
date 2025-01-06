package com.example.hadoopdemo.mapmatching.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MyWritable implements WritableComparable<MyWritable>  {
	public Text vehicleID;
	public LongWritable date;
	public MyWritable(){
		
	}
	public MyWritable(String key, long value){
		vehicleID=new Text(key);
		date=new LongWritable(value);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
//		vehicleID.readFields(in);  
//		date.readFields(in);  
		vehicleID = new Text(in.readUTF());
        date = new LongWritable(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
//		vehicleID.write(out);
//		date.write(out);
		out.writeUTF(vehicleID.toString());
        out.writeLong(date.get());
	}
    @Override  
    public int hashCode() {
        return vehicleID.hashCode();  
    }  
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
//		myWritable o = (myWritable)obj;
//        return this.vehicleID o.vehicleID;//&& this.date == o.date
        
        if(obj instanceof MyWritable){
        	MyWritable o = (MyWritable)obj;
            return this.vehicleID.equals(o.vehicleID);//&& this.date.equals(o.date);
        }
        return false;
	}
	@Override
	public int compareTo(MyWritable o) {
		int result = this.vehicleID.compareTo(o.vehicleID);
        if(result != 0)
            return result;
        return this.date.compareTo(o.date);
	}
	
	@Override
	public String toString(){
        return this.vehicleID.toString()+","+this.date.toString();
    }

	public Text getVehicleID() {
		return vehicleID;
	}

	public void setVehicleID(Text vehicleID) {
		this.vehicleID = vehicleID;
	}

	public LongWritable getDate() {
		return date;
	}

	public void setDate(LongWritable date) {
		this.date = date;
	}
}
