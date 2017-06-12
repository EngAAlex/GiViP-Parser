/**
 * 
 */
package com.unipg.hdfs2sql.db;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author maria
 */

public class ConnectionFactory {

	public static final String dbName = "AvailableJobs";

	//static reference to itself
	private static ConnectionFactory instance;
	private Connection openConnection;

	private Properties properties;
	private static final String propFileName = "application.properties";
	private String dburl;// = "jdbc:mysql://localhost:3306/";
	private String dbuser;// = "root";
	private String dbpassw;// = "789456";
	private String dboptions;
	public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver"; 

	//private constructor
	private ConnectionFactory() {

		properties = new Properties();

		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		try {
			Class.forName(DRIVER_CLASS);
			if (inputStream != null) {
				properties.load(inputStream);
				dburl = properties.getProperty("dbconnection.string", null);
				dboptions = properties.getProperty("dbconnection.options", null);
				dbuser = properties.getProperty("dbconnection.user", null);
				dbpassw = properties.getProperty("dbconnection.password", null);				
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException ioe){
			ioe.printStackTrace();
		}
	}

	private static ConnectionFactory getInstance(){
		if(instance == null)
			instance = new ConnectionFactory();
		return instance;
	}

	private Connection getOpenConnection() {
		return openConnection;
	}

	//	private void setOpenConnection(Connection openConnection) {
	//		this.openConnection = openConnection;
	//	}

	//	private Connection createConnection() {
	//		Connection connection = null;
	//		try {
	//			connection = DriverManager.getConnection(dburl, dbuser, dbpassw);
	//		} catch (SQLException e) {
	//			e.printStackTrace();
	//			System.out.println("ERROR: Unable to Connect to Database.");
	//		}
	//		return connection;
	//	}  


	private void createConnection(String db) {
		String dbURL = dburl+db;//+"?"+dboptions;
//		String dbURL = dburl;
		try {
			openConnection = DriverManager.getConnection(dbURL, dbuser, dbpassw);
			openConnection.setAutoCommit(false);
			//      System.out.println("Connection complete");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("ERROR: Unable to Connect to Database.");
		}		
	}

	public static void closeConnection() throws SQLException{
		if(!getInstance().checkForClosedConnection())
			getInstance().getOpenConnection().close();
	}
	
	public static void closeStatement(PreparedStatement statement){
		if(statement != null)
			try {
				statement.close();
			} catch (SQLException e) {
				//e.printStackTrace();
			}
	}

	public boolean checkForClosedConnection(){
		try {
			return getOpenConnection() == null || getOpenConnection().isClosed();
		} catch (SQLException e) {
			return true;
		}
	}

	public static PreparedStatement prepare(String query) throws NullPointerException, SQLException{
		if(!getInstance().checkForClosedConnection())
			return getInstance().getOpenConnection().prepareStatement(query);
		throw new NullPointerException();
	}

	public static void beginTransaction(String db) throws SQLException{		
		if(getInstance().checkForClosedConnection())
			getInstance().createConnection(db);
	}

	public static void commit(boolean close) throws SQLException{
		if(getInstance().checkForClosedConnection())
			throw new SQLException("Connection uninitialized");
		try {
			getInstance().getOpenConnection().commit();
			if(close)
				getInstance().getOpenConnection().close();
		} catch (SQLException e) {
			if(!getInstance().checkForClosedConnection())
				try {
					getInstance().getOpenConnection().rollback();
					getInstance().getOpenConnection().close();				
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			throw new SQLException();
		}
		//		getInstance().setOpenConnection(createConnection(db));
	}
}
