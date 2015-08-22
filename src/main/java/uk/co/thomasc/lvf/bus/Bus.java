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

import uk.co.thomasc.lvf.Main;
import uk.co.thomasc.lvf.Stats;
import uk.co.thomasc.lvf.TFL;
import uk.co.thomasc.lvf.bus.destination.DestinationTask;
import uk.co.thomasc.lvf.packets.BusPacket;
import uk.co.thomasc.lvf.packets.Packet3Flush;
import uk.co.thomasc.lvf.packets.TaskPacket;

/**
 * A bus tracked by TFL
 * This class is kept as minimal as possible
 * to lower the chance of concurrency issues
 */
public class Bus {
	
	/**
	 * A map of vid to its Bus object for quick lookup from the main thread
	 */
	private static Map<Integer, Bus> singleton = new HashMap<Integer, Bus>();
	/**
	 * A map of uvi to its Bus object for quick lookup
	 * for Tasks and History initialisation
	 */
	private static Map<Integer, Bus> singletonUvi = new HashMap<Integer, Bus>();
	/**
	 * Queue of Buses for processing, ordered by
	 * the time of the next prediction for each
	 */
	private static PriorityQueue<Bus> queue = new PriorityQueue<Bus>(11, new Comparator<Bus>() {
		@Override
		public int compare(Bus o1, Bus o2) {
			return o1.getHead().before(o2.getHead()) ? -1 : 1;
		}
	});

	/**
	 * Return a Bus object given a uvi
	 * The bus is loaded if not already cached
	 * 
	 * @param uvi The unique vehicle identifier
	 * @return A Bus object
	 * @throws SQLException If we have a problem loading the Bus
	 */
	public static Bus getFromUvi(int uvi) throws SQLException {
		if (!singletonUvi.containsKey(uvi)) {
			final PreparedStatement stmt = Main.sql.query("SELECT cdreg, uvi, vid FROM vehicles WHERE uvi = ?", new Object[] {uvi});
			final ResultSet r = stmt.getResultSet();
			if (r.next()) {
				new Bus(r);
			}
			stmt.close();
		}
		return singletonUvi.get(uvi);
	}

	/**
	 * Return a Bus object given a TFL vid
	 * The bus is loaded if not already cached
	 * 
	 * @param vid The vehicle identifier
	 * @return A bus object
	 */
	public static Bus getFromVid(int vid) {
		if (!singleton.containsKey(vid)) {
			singleton.put(vid, new Bus(vid));
		}
		return singleton.get(vid);
	}

	/**
	 * Check the bus queue to see if any buses need processing
	 * Buses need processing if their next prediction is now in the past
	 */
	public static void checkQueue() {
		final Date now = new Date();
		// Loop through buses until they are no longer in the past
		while (!queue.isEmpty() && queue.peek().getHead().before(now)) {
			// Update the predictions for the bus
			queue.poll().updateQueue();
		}
	}

	/**
	 * The TFL vid of the Bus
	 */
	private int vid;
	/**
	 * The unique vehicle identifier assigned by LVF
	 */
	private int uvi;
	/**
	 * The registration of the vehicle from countdown
	 */
	private String reg;
	/**
	 * If the given vid or uvi has valid vehicle data
	 */
	private boolean exists = true;

	/**
	 * A cache of vehicle History so we only run queries when needed
	 */
	private final Map<HistoryKey, History> history = new HashMap<HistoryKey, History>();
	/**
	 * A queue of {@link Prediction}s for this vehicle ordered by time
	 */
	private final PriorityQueue<Prediction> predictions = new PriorityQueue<Prediction>(11, new Comparator<Prediction>() {
		@Override
		public int compare(Prediction o1, Prediction o2) {
			return o1.getTime().before(o2.getTime()) ? -1 : 1;
		}
	});
	/**
	 * Hashmap to check predictions for significant updates
	 * meaning we only do queue rebuilds when required
	 */
	private final Map<String, Prediction> pred_update = new HashMap<String, Prediction>();

	/**
	 * Bus constructor, loads LVF data
	 * @param vid The TFL vehicle identifier
	 */
	private Bus(int vid) {
		this.vid = vid;
		
		// Load the registration and LVF uvi for the vehicle
		final PreparedStatement stmt = Main.sql.query("SELECT cdreg, uvi FROM vehicles WHERE vid = ? LIMIT 1", new Object[] {vid});
		try {
			final ResultSet vehicle = stmt.getResultSet();
			if (vehicle != null && vehicle.next()) {
				this.reg = vehicle.getString("cdreg");
				this.uvi = vehicle.getInt("uvi");
				
				// Put this object into uvi cache,
				// should already be in the other cache
				singletonUvi.put(this.uvi, this);
			} else {
				// Vehicle doesn't exist!
				this.exists = false;
			}
		} catch (final Exception e) {
			Main.logger.log(Level.WARNING, "Error creating vehicle. Setting exists = false", e);
			this.exists = false;
		} finally {
			try {
				stmt.close();
			} catch (final SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Bus constructor from an LVF vehicle record
	 * @param vehicle ResultSet from vehicles query
	 * @throws SQLException If getting values from query fails
	 */
	public Bus(ResultSet vehicle) throws SQLException {
		this.reg = vehicle.getString("cdreg");
		this.uvi = vehicle.getInt("uvi");
		
		// Put this object into caches
		singletonUvi.put(this.uvi, this);

		this.vid = vehicle.getInt("vid");
		if (!(vehicle.wasNull() || singleton.containsKey(this.vid))) {
			singleton.put(this.vid, this);
		}
	}

	/**
	 * Get a free uvi for a new vehicle
	 * @param guess A desired uvi, often the vid
	 * @return An available uvi
	 * @throws SQLException If we have a problem
	 */
	private int getNewVid(int guess) throws SQLException {
		// Increment by how much, initially 0
		int inc = 0;
		// Number of records with the guessed uvi, hopefully 0
		int c = 0;
		
		do {
			// Check if guess is in use
			PreparedStatement stmt = Main.sql.query("SELECT COUNT(uvi) as c FROM vehicles WHERE uvi = ?", new Object[] {guess});
			ResultSet r = stmt.getResultSet();
			r.next();
			c = r.getInt("c");
			stmt.close();

			if (c > 0) {
				// Increment counter by calling SQL function
				// First loop doesn't increment, just retrieves current value
				stmt = Main.sql.query("SELECT incCounter('uvi', ?)", new Object[] {inc});
				r = stmt.getResultSet();
				r.next();
				
				// Set our new guess to the counter value
				guess = r.getInt("inccounter");
				inc = 1;
				
				stmt.close();
			}
		} while (c > 0);
		return guess;
	}

	/**
	 * Called when a row from TFL references this vehicle
	 * @param tfl A TFL row
	 * @return If the row was "interesting"
	 */
	public boolean newData(TFL tfl) {
		// Do prediction updates for all buses
		checkQueue();

		try {
			// Check if this row includes a change in the vehicle data
			this.checkVehicle(tfl);
		} catch (final Exception e) {
			Main.logger.log(Level.WARNING, "Error in checkVehicle", e);
		}

		// Get the new prediction
		Prediction pred = tfl.getPrediction();
		
		// If we have a cached version of this prediction
		if (this.pred_update.containsKey(pred.getStop())) {
			// Get the cached prediction
			Prediction oldPred = this.pred_update.get(pred.getStop());
			
			// Check if the new prediction is distinct
			// {@see Predicition#equals} only checks valid and difftime fields
			if (oldPred.equals(pred)) {
				// It's not interesting
				return false;
			}
			
			// Remove the old prediction from the queue
			this.predictions.remove(oldPred);
		}
		// Put the new predicition in the cache and queue
		this.pred_update.put(pred.getStop(), pred);
		this.predictions.offer(pred);

		// Do prediction updates for this bus,
		// our new prediction might already need processing
		this.updateQueue();
		
		// If we came this far it was interesting!
		return true;
	}

	/**
	 * Combine the date part of a date with a time
	 * ie 15:01:12 + 2015-02-15 12:23:14 = 2015-02-15 15:01:12
	 * @param time A time
	 * @param today A date
	 * @return The time on the date
	 */
	private Date timeToDate(Time time, Date today) {
		final Calendar cal = new GregorianCalendar();
		cal.setTime(today);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.MILLISECOND, (int) time.getTime());
		return cal.getTime();
	}

	/**
	 * Update the vehicle history to include a time
	 * @param date A date in the format "yyyy-MM-dd" {@see TFL#dateFormat}
	 * @param time A time to include in the history
	 * @param route The route for this history
	 * @param lineid The lineid for this history
	 */
	private void updateHistory(String date, Date time, String route, String lineid) {
		try {
			boolean update = false;
			// Create key for map
			final HistoryKey key = new HistoryKey(date, lineid, route);

			// If we have no history for this date, load it from the database
			if (!this.history.containsKey(key)) {
				final PreparedStatement stmt = Main.sql.query("SELECT * FROM route_day WHERE vid = ? AND date = ? AND route = ? AND lineid = ? LIMIT 1", new Object[] {this.uvi, new java.sql.Date(time.getTime()), route, lineid});
				final ResultSet r = stmt.getResultSet();

				final History hist = new History();
				if (r.next()) {
					Date t = null;
					if ((t = this.timeToDate(r.getTime("first_seen"), time)) != null) {
						hist.setFirstSeen(t);
					}
					if ((t = this.timeToDate(r.getTime("first_seen"), time)) != null) {
						hist.setLastSeen(t);
					}
				}
				stmt.close();
				this.history.put(key, hist);
			}

			// If we have no first seen value or new value is earlier
			if (this.history.get(key).getFirstSeen() == null || this.history.get(key).getFirstSeen().after(time)) {
				this.history.get(key).setFirstSeen(time);
				update = true;
			}
			// If we have no last seen value or new value is later
			if (this.history.get(key).getLastSeen() == null || this.history.get(key).getLastSeen().before(time)) {
				this.history.get(key).setLastSeen(time);
				update = true;
			}

			// If we made a change to the history object, save it
			if (update) {
				Object[] vars = new Object[] {route, new java.sql.Time(this.history.get(key).getFirstSeen().getTime()), new java.sql.Time(this.history.get(key).getLastSeen().getTime()), this.reg, this.uvi, lineid, new java.sql.Date(time.getTime())};
				
				// Function call does INSERT OR UPDATE, in future versions of postgres this will supported as actual SQL
				Main.sql.function("{call update_route_day (?, ?, ?, ?, ?, ?, ?)}", vars);
			}
		} catch (final SQLException e) {
			Main.logger.log(Level.WARNING, "Error updating history", e);
		}
	}

	/**
	 * Check for changes in vehicle information
	 * 
	 * New vehicles and registration changes should be processed here
	 * 
	 * @param tfl Data from TFL
	 * @throws Exception If we have a problem
	 */
	private void checkVehicle(TFL tfl) throws Exception {
		if (!this.exists) { // If this is a vid that isn't known by LVF
			final PreparedStatement stmt = Main.sql.query("SELECT * FROM vehicles WHERE cur_reg = ?", new Object[] {tfl.getReg()});
			final ResultSet old = stmt.getResultSet();

			if (old.next()) { // Registration has changed vid
				final int uvi = this.getNewVid(this.vid);
				if (old.getBoolean("pre")) {
					// Pre-populated by cur_reg
					Main.logger.log(Level.INFO, "1Prepopulated Vehicle (" + old.getString("cur_reg") + ", " + uvi + ")");
					Main.sql.update("UPDATE vehicles SET pre = FALSE, vid = ?, uvi = ?, cdreg = ? WHERE cur_reg = ?", new Object[] {this.vid, uvi, tfl.getReg(), tfl.getReg()});

					this.exists = true;
					this.reg = tfl.getReg();
					this.uvi = uvi;
				} else {
					// Shared code, {@see #preEntered}
					this.preEntered(tfl, old.getInt("uvi"), uvi);
				}
			} else { // Completely unknown vehicle
				this.doInsert(tfl, this.getNewVid(this.vid));
			}
			stmt.close();
		} else if (!tfl.getReg().equals(this.reg)) { // Known vid, but the registration changed
			final boolean hasDot = tfl.getReg().contains(".");
			
			// Check if we need to keep this vehicle
			final PreparedStatement stmt = Main.sql.query("SELECT keep FROM vehicles WHERE vid = ?", new Object[] {this.vid});
			final ResultSet vehicle = stmt.getResultSet();
			vehicle.next();

			if (!vehicle.getBoolean("keep") || hasDot) {
				// Withdraw the vehicle that used to have this registration
				final PreparedStatement stmt2 = Main.sql.query("SELECT vid FROM vehicles WHERE cdreg = ?", new Object[] {this.reg});
				ResultSet oldVeh = stmt2.getResultSet();
				
				if (oldVeh.next()) {
					getFromVid(oldVeh.getInt("vid")).withdrawVehicle();
				}

				// Update this vid's cdreg
				Main.logger.log(Level.INFO, "Changed Vid (" + this.reg + ", " + this.uvi + ")");
				Main.sql.update("UPDATE vehicles SET cdreg = ? WHERE vid = ?", new Object[] {tfl.getReg(), this.vid});
				this.reg = tfl.getReg();
			} else {
				// We want to keep this record, not update its cdreg, so withdraw it
				final int uvi = this.getNewVid(this.vid);
				this.withdrawVehicle();

				// Check for pre-populated values for this registration
				final PreparedStatement stmt2 = Main.sql.query("SELECT * FROM vehicles WHERE cur_reg = ?", new Object[] {tfl.getReg()});
				final ResultSet old = stmt2.getResultSet();
				if (old != null && old.next()) {
					if (old.getBoolean("pre")) {
						// Pre-populated by cur_reg
						Main.logger.log(Level.INFO, "2Prepopulated Vehicle (" + old.getString("cur_reg") + ", " + uvi + ")");
						Main.sql.update("UPDATE vehicles SET pre = FALSE, vid = ?, uvi = ?, cdreg = ? WHERE cur_reg = ?", new Object[] {this.vid, uvi, tfl.getReg(), tfl.getReg()});

						this.uvi = uvi;
						this.reg = tfl.getReg();
					} else {
						// Shared code, {@see #preEntered}
						this.preEntered(tfl, old.getInt("uvi"), uvi);
					}
				} else {
					// Insert a new record for this vehicle
					this.doInsert(tfl, uvi);
				}
				stmt2.close();
			}
			stmt.close();
		}
	}

	/**
	 * Withdraw the vehicle
	 * 
	 * Removes the where_seen data, but not history
	 * 
	 * NULLs the vid as this is countdown information
	 * that doesn't refer to withdrawn vehicles
	 * 
	 * @param field Key to withdraw by
	 * @param value Value of the key to withdraw
	 * @return The vid of the withdrawn vehicle
	 * @throws SQLException If the withdraw fails
	 */
	private void withdrawVehicle() throws SQLException {
		// Clear data cached in this object
		this.history.clear();
		this.pred_update.clear();
		this.predictions.clear();
		queue.remove(this);

		// Remove from map and remove data
		singleton.remove(this.vid);
		this.exists = false;
		this.reg = "";
		this.vid = 0;

		// Delete where seen
		Main.sql.update("DELETE FROM where_seen WHERE vid = ?", new Object[] {uvi});
		// Remove vid from this row
		Main.sql.update("UPDATE vehicles SET vid = NULL, cdreg = NULL WHERE uvi = ?", new Object[] {uvi});

		Main.logger.log(Level.INFO, "Withdrawn vehicle (" + reg + ", " + uvi + ")");
	}

	/**
	 * Record with this reg is immutable, but a new vid owns the registration
	 * withdraw the old vehicle and check for orig_reg pre-populated vehicles
	 * 
	 * If none found, insert this new vehicle.
	 * 
	 * @param tfl Data from TFL
	 * @param oldUvi The uvi to withdraw
	 * @param newUvi An available uvi for our new record if needed
	 * @throws SQLException If there's a problem
	 */
	private void preEntered(TFL tfl, int oldUvi, int newUvi) throws SQLException {
		// Withdraw the old record as we now own this registration
		getFromUvi(oldUvi).withdrawVehicle();

		// Check for other pre-populated values by orig_reg
		final PreparedStatement stmt = Main.sql.query("SELECT * FROM vehicles WHERE orig_reg = ? AND pre = TRUE LIMIT 1", new Object[] {tfl.getReg()});
		try {
			final ResultSet pre = stmt.getResultSet();
			if (pre != null && pre.next()) {
				Main.logger.log(Level.INFO, "3Prepopulated Vehicle (" + pre.getString("cur_reg") + ", " + newUvi + ")");
				Main.sql.update("UPDATE vehicles SET pre = FALSE, vid = ?, uvi = ?, keep = 1, cdreg = ? WHERE orig_reg = ? AND pre", new Object[] {this.vid, newUvi, tfl.getReg(), tfl.getReg()});
				this.exists = true;
				this.reg = tfl.getReg();
				this.uvi = newUvi;
			} else {
				// No pre-populated values found, insert a new row
				this.doInsert(tfl, this.getNewVid(this.vid));
			}
		} finally {
			stmt.close();
		}
	}

	/**
	 * Insert a new vehicle record
	 * @param tfl Data from TFL
	 * @param uvi An available uvi for our new record
	 */
	private void doInsert(TFL tfl, int uvi) {
		// Record this event
		Stats.event("insert_veh");
		Main.logger.log(Level.INFO, "New vehicle " + uvi + " (" + tfl.getReg() + ")");

		// Do the insert
		Main.sql.update("INSERT INTO vehicles (vid, uvi, cdreg, cur_reg, orig_reg, operator) VALUES (?, ?, ?, ?, ?, ?)", new Object[] {this.vid, uvi, tfl.getReg(), tfl.getReg(), tfl.getReg(), "UN"});
		
		// Add to the new list
		try {
			Main.sql.update("INSERT INTO lists (list_name, vid) VALUES (?, ?)", new Object[] {"new", this.vid});
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Update this object's state
		this.exists = true;
		this.uvi = uvi;
		this.reg = tfl.getReg();
	}

	/**
	 * Get the time of the prediction next to happen for this vehicle
	 * @return The time for the next prediction
	 */
	private Date getHead() {
		return this.predictions.peek().getTime();
	}

	/**
	 * Process predictions that have happened into history and where_seen
	 */
	private void updateQueue() {
		// Remove ourselves from the queue as at the end of this
		// function we may have a different position in the queue
		// To move in a queue you need to re-offer
		queue.remove(this);

		final Date now = new Date();
		// Remove predicitons that have happened
		while (!this.predictions.isEmpty() && (this.predictions.peek().getTime().before(now) || !this.predictions.peek().isValid())) {
			final Prediction pred = this.predictions.poll();
			this.pred_update.remove(pred.getStop());
		}

		// If we have no more predictions ignore this
		if (!this.predictions.isEmpty()) {
			// Get next prediction to happen that's still in the future
			final Prediction pred = this.predictions.peek();
			// Update the history of this vehicle to include this prediction as this is when it's most accurate
			this.updateHistory(pred.getKeytime(), pred.getTime(), pred.getRoute(), pred.getLineid());
			
			// Function call does INSERT OR UPDATE, in future versions of postgres this will supported as actual SQL
			Main.sql.function("{call update_where_seen (?, ?, ?, ?::timestamp, ?, ?::smallint, ?, ?::smallint)}", pred.toDbObject(this.uvi));
			
			// Increment the instances of this destination on the route
			DestinationTask.incrementCount(pred.getRoute(), pred.getLineid(), pred.getDirid(), pred.getDest());

			// Re-add ourselves to the queue to be processed when this prediction has happened
			queue.offer(this);
		}
	}

	/**
	 * Load the history for this vehicle from a ResultSet from the database
	 * @param r A ResultSet containing history rows
	 */
	public void initHistory(ResultSet r) {
		try {
			// Create a history and a history key from the row
			final History hist = new History(this.timeToDate(r.getTime("first_seen"), r.getDate("date")), this.timeToDate(r.getTime("last_seen"), r.getDate("date")));
			final HistoryKey key = new HistoryKey(TFL.dateFormat.format(r.getDate("date")), r.getString("lineid"), r.getString("route"));

			// Store this information
			this.history.put(key, hist);
		} catch (final SQLException e) {
			Main.logger.log(Level.WARNING, "Error initialising history. This will hurt performace as they will be loaded manually.", e);
		}
	}

	/**
	 * Clear all references to vehicle objects
	 * and reload them from the database
	 * 
	 * This is far more costly than flushing vehicles individually
	 * which should be done when possible
	 * 
	 * @throws SQLException Error when loading data
	 */
	public static void flush() throws SQLException {
		singleton.clear();
		singletonUvi.clear();
		queue.clear();
		
		// Read vehicles table, that are active (have vids),
		// read cdreg, uvi & vid
		// This speeds up startup where active vehicles would get loaded
		// one at a time as they are referenced by updates
		PreparedStatement stmt = Main.sql.query("SELECT cdreg, uvi, vid FROM vehicles WHERE vid IS NOT NULL");
		ResultSet c = stmt.getResultSet();
		int loaded = 0;
		while (c.next()) {
			loaded++;
			new Bus(c);
		}
		Main.logger.log(Level.INFO, "Loaded " + loaded + " vehicles");
		stmt.close();

		// load current day history records for active vehicles
		stmt = Main.sql.query("SELECT * FROM route_day WHERE date = current_date ORDER BY vid ASC");
		c = stmt.getResultSet();
		while (c.next()) {
			final Bus bus = Bus.getFromUvi(c.getInt("vid"));
			if (bus != null) {
				bus.initHistory(c);
			}
		}
		stmt.close();
	}

	/**
	 * Perform a task requested by the web interface
	 * @param task The packet containing task parameters
	 * @throws Exception Anything could happen
	 */
	public void performTask(BusPacket task) throws Exception {
		if (task instanceof Packet3Flush) {
			// Remove references to this object, it will be recreated when needed
			singleton.remove(this.vid);
			singletonUvi.remove(this.uvi);
			queue.remove(this);
			
			this.exists = false;
			this.reg = "";
			this.vid = 0;
			this.uvi = 0;
		}
	}

}
