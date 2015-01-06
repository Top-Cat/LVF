package uk.co.thomasc.lvf;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.logging.Level;

import com.google.gson.JsonObject;

public class Sql {

	private final String url;
	private Connection conn;

	public Sql(JsonObject sql) {
		this.url = "jdbc:mysql://" + sql.get("server").getAsString() + "/" + sql.get("db").getAsString() + "?user=" + sql.get("user").getAsString() + "&password=" + sql.get("pass").getAsString() + "&autoReconnect=true&failOverReadOnly=false&maxReconnects=10";

		try {
			Main.logger.log(Level.INFO, "Connecting to SQL server");
			this.reconnect();
			Main.logger.log(Level.INFO, "Connected");
		} catch (final Exception e) {
			Main.logger.log(Level.WARNING, "Error connecting to SQL server", e);
		}
	}

	private void reconnect() throws Exception {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		this.conn = DriverManager.getConnection(this.url);
	}

	private boolean checkConnection() throws SQLException {
		return this.conn != null && !this.conn.isClosed();
	}

	private void checkAndFix() throws Exception {
		if (!this.checkConnection()) {
			this.reconnect();
		}
	}

	public void update(String sql) {
		this.update(sql, null);
	}

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

	public void insert(String sql) {
		this.insert(sql, null);
	}

	public void insert(String sql, Object[] values) {
		try {
			this.checkAndFix();
			final PreparedStatement pr = this.conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			if (values != null) {
				for (int i = 1; i <= values.length; i++) {
					this.setValue(pr, i, values[i - 1]);
				}
			}
			pr.executeUpdate();
			pr.close();
		} catch (final Exception e) {
			Main.logger.log(Level.WARNING, "Error performing SQL insert (" + sql + ")", e);
		}
	}

	public PreparedStatement query(String sql) {
		return this.query(sql, null);
	}

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
