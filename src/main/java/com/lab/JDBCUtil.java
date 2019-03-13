package com.lab;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class JDBCUtil {
	 
	public static ComboPooledDataSource cpds;
	static {
		//1.初始化C3P0数据源
		  cpds = new ComboPooledDataSource();
		// 设置连接数据库需要的配置信息
		try {
			cpds.setDriverClass("com.mysql.jdbc.Driver");
			cpds.setJdbcUrl("jdbc:mysql://twl:3306/goods?useSSL=false");
			cpds.setUser("root");
			cpds.setPassword("123456");
			//2.设置连接池的参数
			cpds.setInitialPoolSize(5);
			cpds.setMaxPoolSize(15);
			 
		} catch (Exception e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	public static DataSource getDataSource() {
		return cpds;
	}

	public static Connection getConnection() throws SQLException {
		return cpds.getConnection();
	}
}
