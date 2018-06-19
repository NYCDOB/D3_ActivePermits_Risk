package gov.nyc.buildings.strategic.sitesafetyautomation.connection;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectionFactory {
	private static final Logger logger = LogManager.getLogger(ConnectionFactory.class);
	private static final String obieeDriver = "oracle.bi.jdbc.AnaJdbcDriver";
	private static final String postgresqlDriver = "org.postgresql.Driver";
	private static Properties logCredential = null;
		
	public static Connection getConnection(){
		return getConnection("obiee");
	}

	public static Connection getConnection(String driver) {
		Connection con = null;
		if(driver.equalsIgnoreCase("obiee")){
			try {
				logger.debug("starting connect to OBIEE");
				Class.forName(obieeDriver);
				logCredential = new Properties();
				logCredential.load(new FileInputStream("resource/obiee.properties"));
				String host = "jdbc:oraclebi://sprdorabiw-hq.buildings.nycnet:9703/";
				con =  DriverManager.getConnection(host, logCredential);
				logger.debug("connection to OBIEE established");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
		} else if (driver.equalsIgnoreCase("postgresql")) {
			try {
				Class.forName(postgresqlDriver);
				logCredential = new Properties();
				logCredential.load(new FileInputStream("resource/postgresql.properties"));
				String host = "jdbc:postgresql://localhost:5432/transiant_analysis";
				con =  DriverManager.getConnection(host, logCredential);
				logger.debug("connection to PostgreSQL established");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
			
		} else if (driver.equalsIgnoreCase("postgresql_server")) {
			try {
				Class.forName(postgresqlDriver);
				logCredential = new Properties();
				logCredential.load(new FileInputStream("resource/postgresql.properties"));
				String host = "jdbc:postgresql://10.154.69.166:5432/transiant_analysis";
				con =  DriverManager.getConnection(host, logCredential);
				logger.debug("connection to PostgreSQL established");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
			
		} else {
			logger.error("Not yet have driver and JDBC specified, returning null");
			
		}
		return con;
		
	}

}
