package com.buysidefx.database;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import com.buysidefx.exceptions.DatabaseException;
import com.buysidefx.props.PropertiesLoader;



public enum DatabaseUtilitySingleton {
	INSTANCE;
	private static final Logger logger = Logger
			.getLogger(DatabaseUtilitySingleton.class);

	private static GenericObjectPool<Connection> poolMgr = null;

	private static boolean debug = false;

	static {

		String startString = DatabaseUtilitySingleton.class.getCanonicalName()
				+ " static initializer";
		logger.info(startString);

		Properties props = PropertiesLoader.loadProperties("database", null);
		String serverName = props.getProperty("serverName");
		if (serverName == null)
			serverName = "localhost";
		String portStr = props.getProperty("port");
		if (portStr == null)
			portStr = "8082";
		int port = Integer.parseInt(portStr);
		String databaseName = props.getProperty("databaseName");
		if (databaseName == null)
			databaseName = "bfx";
		String user = props.getProperty("user");
		if (user == null)
			user = "marathon";
		String password = props.getProperty("password");
		if (password == null)
			password = "marathon";
		int maxConnections = 100;


		String debugStr = props.getProperty("debug");
		if (debugStr != null) {
			if (debugStr.toUpperCase(). equals("TRUE")) {
				debug = true;
			}
		}


		String url = "jdbc:postgresql://" + serverName + ":" + port + "/" + databaseName;


		try {
			logger.info("Creating ConnectionPoolDataSource with: " + serverName + " " + port + "  " + databaseName + " " + user + " and a password");
			poolMgr = new GenericObjectPool<Connection>(null);
			poolMgr.setMaxActive(maxConnections);
			ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, user, password);
			new PoolableConnectionFactory(connectionFactory, poolMgr, null, null, false, true);

		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}


	}


	private String stackTraceToString(Throwable t) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		t.printStackTrace(pw);
		return sw.toString(); // stack trace as a string
	}

	private Connection getConnection() throws DatabaseException {
		Connection conn = null;
		if (poolMgr != null) {
			synchronized (this) {
				try {
					conn = poolMgr.borrowObject();
				} catch (NoSuchElementException e) {
					logger.error("NoSuchElement Exception in DatabaseUtilitySingleton.INSTANCE.getConnection(): "+stackTraceToString(e),e);
					DatabaseException de = new DatabaseException(e.getMessage(),e);
					throw de;
				} catch (IllegalStateException e) {
					logger.error("IllegalStateException Exception in DatabaseUtilitySingleton.INSTANCE.getConnection(): "+stackTraceToString(e),e);
					DatabaseException de = new DatabaseException(e.getMessage(),e);
					throw de;

				} catch (Exception e) {
					logger.error("General Exception in DatabaseUtilitySingleton.INSTANCE.getConnection(): "+stackTraceToString(e),e);
					DatabaseException de = new DatabaseException(e.getMessage(),e);
					throw de;
				}
			}
		} else {
			logger.error("PoolManager is null in DatabaseUtilitySingleton.INSTANCE.getConnection()");
			DatabaseException de = new DatabaseException("PoolManager is null",null);
			throw de;
		}
		return conn;
	}


	private PreparedStatement getPreparedStatement(Connection conn, String sql) throws DatabaseException {
		try {
			return conn.prepareStatement(sql); // create a statement

		} catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.getPreparedStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.getPreparedStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;
		}

	}


	private void freeResources(ResultSet rs, PreparedStatement pstmt, Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception ignore) {}
		}
		if (pstmt != null) {
			try {
				pstmt.close();
			} catch (Exception ignore) {}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception ignore) {}
		}
	}





}