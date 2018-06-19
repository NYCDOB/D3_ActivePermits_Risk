package gov.nyc.buildings.strategic.sitesafetyautomation.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Util {
	private static final Logger logger = LogManager.getLogger(Util.class);
	
	public static <T> void close(T t){
		if (t instanceof Connection) {
			try {
				((Connection) t).close();
				logger.debug("Connection closed");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (t instanceof InputStream){
			try {
				((InputStream) t).close();
				logger.debug("Connection closed");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (t instanceof OutputStream){
			try {
				((OutputStream) t).close();
				logger.debug("Connection closed");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (t instanceof Reader){
			try {
				((Reader) t).close();
				logger.debug("Connection closed");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (t instanceof Writer){
			try {
				((Writer) t).close();
				logger.debug("Connection closed");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
