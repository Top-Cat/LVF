package uk.co.thomasc.lvf;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

import com.google.gson.JsonObject;

/**
 * Abstracts database operations from the rest of the code
 * Has functions to insert, update and query data from a sql database
 * Uses batching to minimise overhead, combining many similar queries
 */
public class Sql extends TimerTask {

	/**
	 * Timer used to execute batches of updates
	 */
	private final Timer t = new Timer();
	/**
	 * Storage of batches to be run
	 */
	private Map<String, CallableStatement> batch = new HashMap<String, CallableStatement>();
	
	/**
	 * The SQl connection string derived from login.json
	 */
	private final String url;
	/**
	 * A persistent SQL connection to execute statements through
	 */
	private Connection conn;

	/**
	 * SQL constructor, opens the connection and starts the batch update task
	 * @param sql The SQL section from login.json with SQL credentials
	 */
	public Sql(JsonObject sql) {
		this.url = "jdbc:postgresql://" + sql.get("server").getAsString() + "/" + sql.get("db").getAsString() + "?user=" + sql.get("user").getAsString() + "&password=" + sql.get("pass").getAsString() + "&autoReconnect=true&failOverReadOnly=false&maxReconnects=10";

		try {
			Main.logger.log(Level.INFO, "Connecting to SQL server");
			this.reconnect();
			Main.logger.log(Level.INFO, "Connected");
		} catch (final Exception e) {
			Main.logger.log(Level.WARNING, "Error connecting to SQL server", e);
		}
		
		final int period = 1000; // 1 seconds
		this.t.schedule(this, period, period);
	}

	/**
	 * Stop the batch update task
	 * called from the main thread if a critical error is experienced
	 */
	public void finish() {
		this.t.cancel();
	}

	/**
	 * Connect to the SQL server
	 * @throws Exception If we fail
	 */
	private void reconnect() throws Exception {
		// Make sure the postgres driver is loaded,
		// otherwise the next line will fail
		Class.forName("org.postgresql.Driver").newInstance();
		
		// Open the connection
		this.conn = DriverManager.getConnection(this.url);
	}

	/**
	 * Check we're still connected
	 * @return TRUE if we're connected to an SQL server
	 * @throws SQLException We're probably not connected
	 */
	private boolean checkConnection() throws SQLException {
		return this.conn != null && !this.conn.isClosed();
	}

	/**
	 * If we're not connected, reconnect
	 * 
	 * Called from most helper functions before
	 * attempting an action on the remote SQL server
	 * 
	 * @throws Exception If we failed
	 */
	private void checkAndFix() throws Exception {
		if (!this.checkConnection()) {
			this.reconnect();
		}
	}

	/**
	 * Execute a function located on the SQL server with no parameters
	 * 
	 * @param sql The SQL query to perform
	 */
	public void function(String sql) {
		this.function(sql, null);
	}

	/**
	 * Execute a function located on the SQL server
	 * 
	 * Functions are batched and will be
	 * executed within the timer period (1 second)
	 * 
	 * @param sql The SQL query to perform
	 * @param values The values of ? variables in the SQL
	 */
	public void function(String sql, Object[] values) {
		try {
			this.checkAndFix();
			
			if (!batch.containsKey(sql)) {
				synchronized (batch) {
					batch.put(sql, this.conn.prepareCall(sql));
				}
			}
			final CallableStatement pr = batch.get(sql);
			
			synchronized (pr) {
				if (values != null) {
					for (int i = 1; i <= values.length; i++) {
						this.setValue(pr, i, values[i - 1]);
					}
				}
				pr.addBatch();
			}

		} catch (final Exception e) {
			Main.logger.log(Level.WARNING, "Error performing SQL function (" + sql + ")", e);
		}
	}

	/**
	 * Executes batched updates every second
	 */
	@Override
	public void run() {
		// Don't allow modification of the batches while we're sending updates
		synchronized (batch) {
			// Loop through statements with batches of variables
			for (CallableStatement pr : batch.values()) {
				// Don't allow additions to this batch while we're executing it
				synchronized (pr) {
					try {
						// Execute the batch
						pr.executeBatch();
					} catch (SQLException e) {
						// If we fail say why and the internal reason too
						e.printStackTrace();
						e.getNextException().printStackTrace();
					}
				}
			}
		}
	}
	
	/**
	 * Perform an update query with no parameters
	 * 
	 * @param sql The SQL query to perform
	 */
	public void update(String sql) {
		this.update(sql, null);
	}

	/**
	 * Perform an update query
	 * Update or insert a table
	 * 
	 * If doing a high volume of changes
	 * consider using function to batch the updates
	 * 
	 * @param sql The SQL query to perform
	 * @param values The values of ? variables in the SQL
	 * @return A PreparedStatement object
	 */
	public void update(String sql, Object[] values) {
		try {
			this.checkAndFix();
			final PreparedStatement pr = this.conn.prepareStatement(sql);
			if (values != null) {
				for (int i = 1; i <= values.length; i++) {
					this.setValue(pr, i, values[i - 1]);
				}
			}
			pr.executeUpdate();
			pr.close();
		} catch (final Exception e) {
			Main.logger.log(Level.WARNING, "Error performing SQL update (" + sql + ")", e);
		}
	}

	/**
	 * Perform a simple query with no parameters
	 * 
	 * @param sql The SQL query to perform
	 * @return A PreparedStatement object
	 */
	public PreparedStatement query(String sql) {
		return this.query(sql, null);
	}

	/**
	 * Perform a simple query
	 * Most basic helper method, constructs and executes some SQL
	 * Remember to close your PreparedStatement
	 * 
	 * @param sql The SQL query to perform
	 * @param values The values of ? variables in the SQL
	 * @return A PreparedStatement object
	 */
	public PreparedStatement query(String sql, Object[] values) {
		try {
			this.checkAndFix();
			final PreparedStatement pr = this.conn.prepareStatement(sql);
			if (values != null) {
				for (int i = 1; i <= values.length; i++) {
					this.setValue(pr, i, values[i - 1]);
				}
			}
			pr.executeQuery();
			return pr;
		} catch (final Exception e) {
			Main.logger.log(Level.WARNING, "Error performing SQL query (" + sql + ")", e);
		}
		return null;
	}

	/**
	 * Coerce given object into variable within a prepared statement
	 * by finding the type and using the correct set method
	 * 
	 * @param pr The prepared statement
	 * @param index The variable index
	 * @param value The value to assign
	 * @throws SQLException If an error occurs while doing the assignment
	 */
	private void setValue(PreparedStatement pr, int index, Object value) throws SQLException {
		if (value instanceof String) {
			pr.setString(index, (String) value);
		} else if (value instanceof Boolean) {
			pr.setBoolean(index, (Boolean) value);
		} else if (value instanceof Integer) {
			pr.setInt(index, (Integer) value);
		} else if (value instanceof Time) {
			pr.setTime(index, (Time) value);
		} else if (value.getClass().equals(Date.class)) { // Is actually java.sql.Date, not java.util.Date
			pr.setDate(index, (Date) value);
		} else if (value instanceof java.util.Date) {
			pr.setTimestamp(index, new Timestamp(((java.util.Date) value).getTime())); // I don't even
		}
	}

}
