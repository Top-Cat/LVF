package uk.co.thomasc.lvf.bus;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Level;

import com.google.gson.JsonObject;

import uk.co.thomasc.lvf.Main;
import uk.co.thomasc.lvf.Stats;
import uk.co.thomasc.lvf.TFL;
import uk.co.thomasc.lvf.bus.destination.DestinationTask;

public class Bus {
	
	private static Map<Integer, Bus> singleton = new HashMap<Integer, Bus>();
	private static Map<Integer, Bus> singletonUvi = new HashMap<Integer, Bus>();
	private static PriorityQueue<Bus> queue = new PriorityQueue<Bus>(11, new Comparator<Bus>() {
		public int compare(Bus o1, Bus o2) {
			return o1.getHead().before(o2.getHead()) ? -1 : 1;
		}
	});
	
	public static Bus getFromUvi(int uvi) throws SQLException {
		if (!singletonUvi.containsKey(uvi)) {
			PreparedStatement stmt = Main.sql.query("SELECT cdreg, uvi, vid FROM lvf_vehicles WHERE uvi = ?", new Object[] {uvi});
			ResultSet r = stmt.getResultSet();
			if (r.first()) {
				new Bus(r);
			}
			stmt.close();
		}
		return singletonUvi.get(uvi);
	}

	public static Bus getFromVid(int vid) {
		if (!singleton.containsKey(vid)) {
			singleton.put(vid, new Bus(vid));
		}
		return singleton.get(vid);
	}
	
	public static void checkQueue() {
		Date now = new Date();
		while (!queue.isEmpty() && queue.peek().getHead().before(now)) {
			queue.poll().updateQueue();
		}
	}

	private int vid;
	private int uvi;
	private String reg;
	private boolean exists = true;
	
	private Map<HistoryKey, History> history = new HashMap<HistoryKey, History>();
	private PriorityQueue<Prediction> predictions = new PriorityQueue<Prediction>(11, new Comparator<Prediction>() {
		public int compare(Prediction o1, Prediction o2) {
			return o1.getTime().before(o2.getTime()) ? -1 : 1;
		}
	});
	private Map<String, Prediction> pred_update = new HashMap<String, Prediction>();
	
	private Bus(int vid) {
		this.vid = vid;
		PreparedStatement stmt = Main.sql.query("SELECT cdreg, uvi FROM lvf_vehicles WHERE vid = ? LIMIT 1", new Object[] {vid});
		try {
			ResultSet vehicle = stmt.getResultSet();
			if (vehicle != null && vehicle.first()) {
				this.reg = vehicle.getString("cdreg");
				this.uvi = vehicle.getInt("uvi");
				singletonUvi.put(this.uvi, this);
			} else {
				// Vehicle doesn't exist!
				exists = false;
			}
		} catch (Exception e) {
			Main.logger.log(Level.WARNING, "Error creating vehicle. Setting exists = false", e);
			exists = false;
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public Bus(ResultSet vehicle) throws SQLException {
		this.reg = vehicle.getString("cdreg");
		this.uvi = vehicle.getInt("uvi");
		singletonUvi.put(uvi, this);
		
		this.vid = vehicle.getInt("vid");
		if (!(vehicle.wasNull() || singleton.containsKey(vid))) {
			singleton.put(vid, this);
		}
	}

	private int getNewVid(int guess) throws SQLException {
		int inc = 0;
		int c = 0;
		do {
			PreparedStatement stmt = Main.sql.query("SELECT COUNT(uvi) as c FROM lvf_vehicles WHERE uvi = ?", new Object[] {guess});
			ResultSet r = stmt.getResultSet();
			r.first();
			c = r.getInt("c");
			stmt.close();
			
			if (c > 0) {
				stmt = Main.sql.query("CALL incCounter('uvi', ?)", new Object[] {inc});
				r = stmt.getResultSet();
				r.first();
				guess = r.getInt("new_val");
				inc = 1;
				stmt.close();
			}
		} while (c > 0);
		return guess;
	}
	
	public boolean newData(TFL tfl) {
		checkQueue();
		
		try {
			checkVehicle(tfl);
		} catch (Exception e) {
			Main.logger.log(Level.WARNING, "Error in checkVehicle", e);
		}
		
		Prediction pred;
		if (pred_update.containsKey(tfl.getStop())) {
			pred = pred_update.get(tfl.getStop());
			if (pred.getDifftime().equals(tfl.getDifftime()) && pred.isValid() == tfl.isValid()) {
				return false;
			}
			predictions.remove(pred);
			
			pred.setRoute(tfl.getRoute());
			pred.setLineid(tfl.getLineid());
			pred.setTime(tfl.getTime());
			pred.setValid(tfl.isValid());
			pred.setKeytime(tfl.getKeytime());
			pred.setDifftime(tfl.getDifftime());
			pred.setDirid(tfl.getDirid());
			pred.setDest(tfl.getDest());
			pred.setVisit(tfl.getVisit());
		} else {
			pred = new Prediction(tfl.getRoute(), tfl.getLineid(), tfl.getTime(), tfl.isValid(), tfl.getKeytime(), tfl.getDifftime(), tfl.getStop(), tfl.getDirid(), tfl.getDest(), tfl.getVisit());
		}
		pred_update.put(tfl.getStop(), pred);
		predictions.offer(pred);
		
		updateQueue();
		return true;
	}
	
	private Date timeToDate(Time time, Date today) {
		Calendar cal = new GregorianCalendar();
		cal.setTime(today);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.MILLISECOND, (int) time.getTime());
		return cal.getTime();
	}
	
	private void updateHistory(String date, Date time, String route, String lineid) {
		try {
			boolean update = false;
			HistoryKey key = new HistoryKey(date, lineid, route);
			
			if (!history.containsKey(key)) {
				PreparedStatement stmt = Main.sql.query("SELECT * FROM lvf_route_day WHERE vid = ? AND date = CURDATE() AND route = ? AND lineid = ? LIMIT 1", new Object[] {uvi, route, lineid});
				ResultSet r = stmt.getResultSet();
				
				History hist = new History();
				if (r.first()) {
					Date t = null;
					if ((t = timeToDate(r.getTime("first_seen"), time)) != null) {
						hist.setFirstSeen(t);
					}
					if ((t = timeToDate(r.getTime("first_seen"), time)) != null) {
						hist.setLastSeen(t);
					}
				}
				stmt.close();
				history.put(key, hist);
			}
			
			if (history.get(key).getFirstSeen() == null || history.get(key).getFirstSeen().after(time)) {
				history.get(key).setFirstSeen(time);
			}
			if (history.get(key).getLastSeen() == null || history.get(key).getLastSeen().before(time)) {
				history.get(key).setLastSeen(time);
				update = true;
			}
			
			if (update) {
				Main.sql.update(
					"REPLACE INTO lvf_route_day (vid, lineid, route, date, last_seen, first_seen) VALUES (?, ?, ?, CURDATE(), ?, ?)",
					new Object[] {this.uvi, lineid, route, new java.sql.Time(history.get(key).getFirstSeen().getTime()), new java.sql.Time(history.get(key).getLastSeen().getTime())}
				);
			}
		} catch (SQLException e) {
			Main.logger.log(Level.WARNING, "Error updating history", e);
		}
	}

	private void checkVehicle(TFL tfl) throws Exception {
		if (!exists) {
			PreparedStatement stmt = Main.sql.query("SELECT * FROM lvf_vehicles WHERE cur_reg = ?", new Object[] {tfl.getReg()});
			ResultSet old = stmt.getResultSet();
			
			if (old.first()) {
				int uvi = getNewVid(vid);
				if (old.getBoolean("pre")) {
					// Pre-populated
					Main.logger.log(Level.INFO, "1Prepopulated Vehicle (" + old.getString("cur_reg") + ", " + uvi + ")");
					Main.sql.update("UPDATE lvf_vehicles SET pre = 0, vid = ?, uvi = ?, cdreg = ? WHERE cur_reg = ?", new Object[] {vid, uvi, tfl.getReg(), tfl.getReg()});
					
					this.exists = true;
					this.reg = tfl.getReg();
					this.uvi = uvi;
				} else {
					// Registration changing vids
					preEntered(tfl, old.getInt("uvi"), uvi);
				}
			} else {
				doInsert(tfl, getNewVid(vid));
			}
			stmt.close();
		} else if (!tfl.getReg().equals(reg)) {
			boolean hasDot = tfl.getReg().contains(".");
			PreparedStatement stmt = Main.sql.query("SELECT keep FROM lvf_vehicles WHERE vid = ?", new Object[] {vid});
			ResultSet vehicle = stmt.getResultSet();
			vehicle.first();
			// Registration change
			if (!vehicle.getBoolean("keep") || hasDot) {
				int vid = withdrawVehicle("cdreg", tfl.getReg());
				if (vid > 0) {
					getFromVid(vid).forceWithdraw();
				}
				
				Main.logger.log(Level.INFO, "Changed Vid (" + reg + ", " + uvi + ")");
				Main.sql.update("UPDATE lvf_vehicles SET cdreg = ? WHERE vid = ?", new Object[] {tfl.getReg(), vid});
				this.reg = tfl.getReg();
			} else {
				int uvi = getNewVid(vid);
				withdrawVehicle("uvi", this.uvi);
				
				PreparedStatement stmt2 = Main.sql.query("SELECT * FROM lvf_vehicles WHERE cur_reg = ?", new Object[] {tfl.getReg()});
				ResultSet old = stmt2.getResultSet();
				if (old != null && old.first()) {
					if (old.getBoolean("pre")) {
						// Pre-populated :D
						Main.logger.log(Level.INFO, "2Prepopulated Vehicle (" + old.getString("cur_reg") + ", " + uvi + ")");
						Main.sql.update("UPDATE lvf_vehicles SET pre = 0, vid = ?, uvi = ?, cdreg = ? WHERE cur_reg = ?", new Object[] {vid, uvi, tfl.getReg(), tfl.getReg()});
						
						this.uvi = uvi;
						this.reg = tfl.getReg();
					} else {
						preEntered(tfl, old.getInt("uvi"), uvi);
					}
				} else {
					doInsert(tfl, uvi);
				}
				stmt2.close();
			}
			stmt.close();
		}
	}

	private void forceWithdraw() {
		singleton.remove(vid);
		this.exists = false;
		this.reg = "";
		this.vid = 0;
	}

	private int withdrawVehicle(String field, Object value) throws SQLException {
		history.clear();
		pred_update.clear();
		predictions.clear();
		queue.remove(this);
		PreparedStatement stmt = Main.sql.query("SELECT * FROM lvf_vehicles WHERE `" + field + "` = ?", new Object[] {value});
		ResultSet oldVehicle = stmt.getResultSet();
		int vid = 0;
		
		if (oldVehicle.first()) {
			Main.sql.update("DELETE FROM lvf_where_seen WHERE vid = ?", new Object[] {oldVehicle.getInt("uvi")});
			Main.sql.update("UPDATE lvf_vehicles SET vid = NULL, cdreg = NULL WHERE uvi = ?", new Object[] {oldVehicle.getInt("uvi")});

			Main.logger.log(Level.INFO, "Withdrawn vehicle (" + oldVehicle.getString("cur_reg") + ", " + oldVehicle.getInt("uvi") + ")");
			vid = oldVehicle.getInt("vid");
		}
		
		stmt.close();
		
		return vid;
	}
	
	private void preEntered(TFL tfl, int oldUvi, int newUvi) throws SQLException {
		withdrawVehicle("uvi", oldUvi);
		
		PreparedStatement stmt = Main.sql.query("SELECT * FROM lvf_vehicles WHERE orig_reg = ? AND pre = 1 LIMIT 1", new Object[] {tfl.getReg()});
		ResultSet pre = stmt.getResultSet();
		if (pre != null && pre.first()) {
			Main.logger.log(Level.INFO, "3Prepopulated Vehicle (" + pre.getString("cur_reg") + ", " + newUvi + ")");
			Main.sql.update("UPDATE lvf_vehicles SET pre = 0, vid = ?, uvi = ?, keep = 1, cdreg = ? WHERE orig_reg = ? AND pre", new Object[] {vid, newUvi, tfl.getReg(), tfl.getReg()});
			this.exists = true;
			this.reg = tfl.getReg();
			this.uvi = newUvi;
		} else {
			doInsert(tfl, getNewVid(vid));
		}
		stmt.close();
	}

	private void doInsert(TFL tfl, int uvi) {
		Stats.event("insert_veh");
		Main.logger.log(Level.INFO, "New vehicle " + uvi + " (" + tfl.getReg() + ")");

		Main.sql.insert("INSERT INTO lvf_vehicles (vid, uvi, cdreg, cur_reg, orig_reg, operator) VALUES (?, ?, ?, ?, ?, ?)", new Object[] {vid, uvi, tfl.getReg(), tfl.getReg(), tfl.getReg(), "UN"});
		Main.sql.insert("INSERT INTO lvf_lists (list_name, vid) VALUES (?, ?)", new Object[] {"new", vid});
		
		this.exists = true;
		this.uvi = uvi;
		this.reg = tfl.getReg();
	}

	private Date getHead() {
		return predictions.peek().getTime();
	}
	
	private void updateQueue() {
		queue.remove(this);
		
		Date now = new Date();
		while (!predictions.isEmpty() && ( predictions.peek().getTime().before(now) || !predictions.peek().isValid())) {
			Prediction pred = predictions.poll();
			pred_update.remove(pred.getStop());
		}
		
		if (!predictions.isEmpty()) {
			Prediction pred = predictions.peek();
			updateHistory(pred.getKeytime(), pred.getTime(), pred.getRoute(), pred.getLineid());
			Main.sql.update("REPLACE INTO lvf_where_seen (vid, route, lineid, last_seen, nearest_stop, dirid, destination, visit) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", pred.toDbObject(this.vid));
			DestinationTask.incrementCount(pred.getRoute(), pred.getLineid(), pred.getDirid(), pred.getDest());
			
			queue.offer(this);
		}
	}

	public void initHistory(ResultSet r) {
		try {
			History hist = new History(timeToDate(r.getTime("first_seen"), r.getDate("date")), timeToDate(r.getTime("last_seen"), r.getDate("date")));
			HistoryKey key = new HistoryKey(TFL.dateFormat.format(r.getDate("date")), r.getString("lineid"), r.getString("route"));
			
			history.put(key, hist);
		} catch (SQLException e) {
			Main.logger.log(Level.WARNING, "Error initialising history. This will hurt performace as they will be loaded manually.", e);
		}
	}

	public void performTask(String object, JsonObject extra) throws Exception {
		if (object.equals("withdraw")) {
			Main.sql.update("DELETE FROM lvf_where_seen WHERE vid = ?", new Object[] {this.uvi});
			Main.sql.update("UPDATE lvf_vehicles SET vid = NULL, cdreg = NULL WHERE uvi = ?", new Object[] {this.uvi});
			forceWithdraw();
		} else if (object.equals("delete")) {
			// First remove linked data
			Main.sql.update("DELETE FROM lvf_history WHERE vid = ?", new Object[] {this.uvi});
			
			// Now delete vehicle record
			Main.sql.update("DELETE FROM lvf_where_seen WHERE vid = ?", new Object[] {this.uvi});
			Main.sql.update("DELETE FROM lvf_vehicles WHERE uvi = ?", new Object[] {this.uvi});
			
			singleton.remove(vid);
			singletonUvi.remove(uvi);
			this.exists = false;
			this.reg = "";
			this.vid = 0;
			this.uvi = 0;
		} else if (object.equals("merge")) {
			int newUvi = extra.get("uvi").getAsInt();
			Bus other = getFromUvi(newUvi);
			if (other != null) {
				Main.sql.update("UPDATE lvf_vehicles SET vid = ?, cdreg = ? WHERE uvi = ?", new Object[] {vid, reg, newUvi}); // Update old record 
				loadHistory();
				other.mergeIn(vid, reg, history, predictions);
				
				Main.sql.update("DELETE FROM lvf_history WHERE vid = ?", new Object[] {this.uvi}); // Delete new record (us)
				Main.sql.update("DELETE FROM lvf_where_seen WHERE vid = ?", new Object[] {this.uvi});
				Main.sql.update("DELETE FROM lvf_vehicles WHERE uvi = ?", new Object[] {this.uvi});
				
				singletonUvi.remove(uvi);
				this.exists = false;
				this.reg = "";
				this.vid = 0;
				this.uvi = 0;
			}
		}
	}

	private void mergeIn(int vid2, String reg2, Map<HistoryKey, History> history2, PriorityQueue<Prediction> predictions2) {
		this.exists = true;
		this.reg = reg2;
		
		singleton.remove(vid);
		singleton.put(vid2, this);
		this.vid = vid2;
		
		// Merge predictions
		while (!predictions2.isEmpty()) {
			Prediction pred = predictions2.poll();
			
			if (pred_update.containsKey(pred.getStop())) {
				predictions.remove(pred_update.get(pred.getStop()));
			}
			
			pred_update.put(pred.getStop(), pred);
			predictions.offer(pred);
		}
		updateQueue();
		
		// Merge history
		for (HistoryKey key : history2.keySet()) {
				updateHistory(key.getDate(), history2.get(key).getFirstSeen(), key.getRoute(), key.getLineid());
				updateHistory(key.getDate(), history2.get(key).getLastSeen(), key.getRoute(), key.getLineid());
		}
	}
	
	private void loadHistory() throws SQLException {
		PreparedStatement stmt = Main.sql.query("SELECT * FROM lvf_route_day WHERE vid = ? AND date >= CURDDATE()", new Object[] {this.uvi});
		ResultSet c = stmt.getResultSet();
		while (c.next()) {
			updateHistory(TFL.dateFormat.format(c.getDate("date")), c.getTime("last_seen"), c.getString("route"), c.getString("lineid"));
			updateHistory(TFL.dateFormat.format(c.getDate("date")), c.getTime("first_seen"), c.getString("route"), c.getString("lineid"));
		}
		stmt.close();
	}
	
}
