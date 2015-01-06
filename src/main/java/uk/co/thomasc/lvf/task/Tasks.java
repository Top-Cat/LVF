package uk.co.thomasc.lvf.task;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import uk.co.thomasc.lvf.Main;

public class Tasks extends Thread {
	
	private boolean running = true;
	private boolean hasTasks = false;
	private Map<Integer, JsonObject> tasks = new HashMap<Integer, JsonObject>();
	private JsonParser parser = new JsonParser();
	
	public Tasks() {
		start();
	}
	
	@Override
	public void run() {
		while (running) {
			try {
				sleep(10000);
				
				synchronized (tasks) {
					PreparedStatement stmt = Main.sql.query("SELECT * FROM lvf_tasks WHERE NOT failed");
					ResultSet c = stmt.getResultSet();
					while (c.next()) {
						hasTasks = true;
						tasks.put(c.getInt("id"), (JsonObject) parser.parse(c.getString("task")));
					}
					stmt.close();
				}
			} catch (Exception e) {
				Main.logger.log(Level.WARNING, "Error getting tasks", e);
			}
		}
	}
	
	public void completed(int id) {
		synchronized (tasks) {
			tasks.remove(id);
			hasTasks = tasks.size() > 0;
			try {
				Main.sql.update("DELETE FROM lvf_tasks WHERE id = ?", new Object[] {id});
			} catch (Exception e) {
				Main.logger.log(Level.WARNING, "Error updating task status", e);
			}
		}
	}
	
	public boolean hasTasks() {
		return hasTasks;
	}
	
	public void finish() {
		running = false;
	}

	public JsonObject[] getTasks() {
		synchronized (tasks) {
			return tasks.values().toArray(new JsonObject[0]);
		}
	}

	public void failed(int id, Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		String exception = sw.toString();
		
		synchronized (tasks) {
			JsonObject extra = tasks.get(id);
			extra.addProperty("exception", exception);
			
			tasks.remove(id);
			hasTasks = tasks.size() > 0;
			
			try {
				Main.sql.update("UPDATE lvf_tasks SET failed = 1 AND task = ? WHERE id = ?", new Object[] {extra, id});
			} catch (Exception e2) {
				Main.logger.log(Level.WARNING, "Error updating task status", e2);
			}
		}
	}
	
}