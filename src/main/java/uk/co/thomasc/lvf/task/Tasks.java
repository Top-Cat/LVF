package uk.co.thomasc.lvf.task;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import uk.co.thomasc.lvf.Main;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Tasks extends Thread {

	private boolean running = true;
	private boolean hasTasks = false;
	private final Map<Integer, JsonObject> tasks = new HashMap<Integer, JsonObject>();
	private final JsonParser parser = new JsonParser();

	public Tasks() {
		this.start();
	}

	@Override
	public void run() {
		while (this.running) {
			try {
				sleep(10000);

				synchronized (this.tasks) {
					final PreparedStatement stmt = Main.sql.query("SELECT * FROM lvf_tasks WHERE NOT failed");
					final ResultSet c = stmt.getResultSet();
					while (c.next()) {
						this.hasTasks = true;
						this.tasks.put(c.getInt("id"), (JsonObject) this.parser.parse(c.getString("task")));
					}
					stmt.close();
				}
			} catch (final Exception e) {
				Main.logger.log(Level.WARNING, "Error getting tasks", e);
			}
		}
	}

	public void completed(int id) {
		synchronized (this.tasks) {
			this.tasks.remove(id);
			this.hasTasks = this.tasks.size() > 0;
			try {
				Main.sql.update("DELETE FROM lvf_tasks WHERE id = ?", new Object[] {id});
			} catch (final Exception e) {
				Main.logger.log(Level.WARNING, "Error updating task status", e);
			}
		}
	}

	public boolean hasTasks() {
		return this.hasTasks;
	}

	public void finish() {
		this.running = false;
	}

	public JsonObject[] getTasks() {
		synchronized (this.tasks) {
			return this.tasks.values().toArray(new JsonObject[0]);
		}
	}

	public void failed(int id, Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		final String exception = sw.toString();

		synchronized (this.tasks) {
			final JsonObject extra = this.tasks.get(id);
			extra.addProperty("exception", exception);

			this.tasks.remove(id);
			this.hasTasks = this.tasks.size() > 0;

			try {
				Main.sql.update("UPDATE lvf_tasks SET failed = 1 AND task = ? WHERE id = ?", new Object[] {extra, id});
			} catch (final Exception e2) {
				Main.logger.log(Level.WARNING, "Error updating task status", e2);
			}
		}
	}

}
