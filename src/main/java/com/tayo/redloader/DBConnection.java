package com.tayo.redloader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBConnection 
{
	static final String DB_URL = "jdbc:redshift://aaaaaaaaaaa.csidc8sx7gmr.us-east-1.redshift.amazonaws.com:5439/xxxxxx";

	private static final Logger logger = LoggerFactory.getLogger(DBConnection.class);

	
	public static Connection getConnection(String user, String pwd)
	{
		Connection conn = null;
		logger.info("Accessing Database connections...");
		try 
		{
			Class.forName("com.amazon.redshift.jdbc41.Driver");
			//Class.forName("org.postgresql.Driver");
			logger.info("Connecting to database...");
			Properties props = new Properties();
			props.setProperty("user", user);
			props.setProperty("password", pwd);
			conn = DriverManager.getConnection(DB_URL, props);			
		} 
		catch (ClassNotFoundException e1) 
		{
			logger.error(e1.toString());
			e1.printStackTrace();
			
		} 
		catch (SQLException e1) 
		{
			logger.error(e1.toString());
			e1.printStackTrace();
			
		}
		return conn;

		
	}

}
