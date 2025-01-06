package com.example.hadoopdemo.mapmatching.jdbc;

import com.example.hadoopdemo.mapmatching.jdbc.JdbcUtils;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

public class CRUD {
	//调用事例
	//sqlString="insert into upload_region values(SEQUENCE_REGION.NEXTVAL,?,?,?,?)";
    //regionID=CRUD.create(sqlString,"regionID", regionsStrings);
//	public static int readTime = 0;
	public static int create(String sql,String id,Object[] params) throws Exception {
		Connection conn = null;
		PreparedStatement pre = null;
		ResultSet rs = null;	
		try {			
			conn = JdbcUtils.getConnection();
			pre = conn.prepareStatement(sql,new String[]{id});	
			for (int i = 1; i <=params.length; i++) 
			{
				pre.setObject(i, params[i-1]);
			}
			pre.executeUpdate();
			ResultSet resultSet=pre.getGeneratedKeys();
	        int num = -1;
			if (resultSet.next()) {
				num = resultSet.getInt(1); 
			}	
			return num;
		}finally {
			JdbcUtils.release(conn, pre, rs);
		}
	}
	//这个create返回的是受影响的行数
	public static int create(String sql,Object[] params) throws Exception {
		Connection conn = null;
		PreparedStatement pre = null;
		ResultSet rs = null;	
		try {			
			conn = JdbcUtils.getConnection();
			pre = conn.prepareStatement(sql);
			ParameterMetaData pmd = pre.getParameterMetaData();
			int paramCount = pmd.getParameterCount();
			check(params,paramCount);
			for (int i = 1; i <=params.length; i++) 
			{
				pre.setObject(i, params[i-1]);	
			}
			int num = -1;
			num=pre.executeUpdate();
			return num;
		} finally {
			JdbcUtils.release(conn, pre, rs);
		}
	}
	public static void BatchCreate(String sql, List<Map<String, Object>> value, String[] keys) throws Exception{
		Connection conn = null;
		PreparedStatement pre = null;
		ResultSet rs = null;	
		try {			
			conn = JdbcUtils.getConnection();
			conn.setAutoCommit(false);//将自动提交关闭
			pre = conn.prepareStatement(sql);
			ParameterMetaData pmd = pre.getParameterMetaData();
			int paramCount = pmd.getParameterCount();
			check(keys,paramCount);
			for (Map<String, Object> map : value) {
				for (int j = 0; j < keys.length; j++) {
					pre.setObject(j + 1, map.get(keys[j]));
				}
				pre.addBatch();
			}
			pre.executeBatch();
			conn.commit();
			pre.clearBatch();
			pre.close();
			conn.setAutoCommit(true);//将自动提交关闭
		} finally {
			JdbcUtils.release(conn, pre, rs);
		}
	}
	//读取
	public static List<Map<String, Object>> read(String sql,Object[] params) throws Exception
	{
		Connection conn = null;
		PreparedStatement pre = null;
		ResultSet rs = null;		
		try {			
			conn = JdbcUtils.getConnection();			
			pre = conn.prepareStatement(sql);
			ParameterMetaData pmd = pre.getParameterMetaData();
			int paramCount = pmd.getParameterCount();
			check(params,paramCount);
			for (int i = 1; i <=paramCount; i++)
			{
				pre.setObject(i, params[i-1]);				
			}
			rs = pre.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int count = rsmd.getColumnCount();
			String[] colNames = new String[count];
			for (int i = 1; i <=count; i++) 
			{				
				colNames[i-1] = rsmd.getColumnLabel(i);
			}
			List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
			Map<String, Object>data = null;
			while(rs.next())
			{
				data = new HashMap<>();
				for (String colName : colNames) {
					data.put(colName, rs.getObject(colName));
				}
				datas.add(data);
			}
			return datas;
		}
		finally{
			JdbcUtils.release(conn, pre, rs);
		}	
	}
	public static <T> List<T> read(String sql,Object[] params,Class<T> clazz) throws Exception
	{
		Connection conn = null;
		PreparedStatement pre = null;
		ResultSet rs = null;
		try {
			conn = JdbcUtils.getConnection();
			pre = conn.prepareStatement(sql);
			ParameterMetaData pmd = pre.getParameterMetaData();
			int paramCount = pmd.getParameterCount();
			check(params,paramCount);
			for (int i = 1; i <=paramCount; i++)
			{
				pre.setObject(i, params[i-1]);
			}
			rs = pre.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int count = rsmd.getColumnCount();
			Set<String> column = new HashSet<>();
			for (int i = 1; i <=count; i++)
			{
				column.add(rsmd.getColumnLabel(i));
			}
			Field[] fields = clazz.getDeclaredFields();
			List<T> datas = new ArrayList<>();
			while(rs.next())
			{
				T data = clazz.newInstance();
				for (Field field : fields) {
					/**忽略编译产生的属性**/
					if (field.isSynthetic())
						continue;
					/**忽略serialVersionUID**/
					if (field.getName().equals("serialVersionUID"))
						continue;
					String colName = field.getName();
					if(!column.contains(colName))
						continue;
					Object o;
					Object oField = rs.getObject(colName);
					if(oField==null){
						continue;
					}
					String v = oField.toString();
					switch (field.getType().getSimpleName()){
						case "String":
						default:
							o = v;
							break;
						case "byte":
							o = Byte.parseByte(v);
							break;
						case "short":
							o = Short.parseShort(v);
							break;
						case "int":
							o = Integer.parseInt(v);
							break;
						case "long":
							o = Long.parseLong(v);
							break;
						case "float":
							o = Float.parseFloat(v);
							break;
						case "double":
							o = Double.parseDouble(v);
							break;
						case "bool":
							o = Boolean.parseBoolean(v);
							break;
					}
					clazz.getDeclaredMethod("set"+colName.substring(0,1).toUpperCase()+colName.substring(1),field.getType()).invoke(data,o);
				}
				datas.add(data);
			}
			return datas;
		}
		finally{
			JdbcUtils.release(conn, pre, rs);
		}
	}
	//更新
	public static int update(String sql,Object[] params) throws Exception{
		return common(sql, params);
	}
	//删除
	public static int delete(String sql,Object[] params) throws Exception{
		return common(sql, params);
	}
	private static int common(String sql,Object[] params) throws Exception{
		Connection conn = null;
		PreparedStatement pre = null;
		try {
			conn = JdbcUtils.getConnection();
			pre = conn.prepareStatement(sql);
			ParameterMetaData pmd = pre.getParameterMetaData();
			int paramCount = pmd.getParameterCount();
			check(params,paramCount);
			for (int i = 1; i <=paramCount; i++)
			{
				pre.setObject(i, params[i-1]);
			}
			return pre.executeUpdate();
		} finally {
			JdbcUtils.release(conn, pre, null);
		}
	}
	private static void check(Object[] params, int paramCount) throws Exception {
		if (null == params )
		{
			if (paramCount != 0)
			{
				System.out.println("params error");
				throw new Exception("params error");
			}
		}
		else if (paramCount != params.length)
		{
			System.out.println(paramCount+" "+params.length);
			System.out.println("params error");
			throw new Exception("params error");
		}
	}

}
