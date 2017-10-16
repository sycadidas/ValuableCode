package com.gome.pop.ScanMysql2Es.es.monitor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

public class DataBaseDao {
	
	protected  final static Logger logger=Logger.getLogger("scanSuccess");  

	
	public  Connection getConnection(String jdbcUrl,String user,String password){
		Connection conn=null;
		try {
			conn=DriverManager.getConnection(jdbcUrl, user, password);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return conn;
	}
	
	
	
	public void executeSql(Connection conn,String sql){
		PreparedStatement statament;
		try {
			statament = conn.prepareStatement(sql);
			statament.execute();
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			try {
				conn.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		
	}
	
}
