package uk.co.thomasc.lvf.bus;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import uk.co.thomasc.lvf.Main;
import uk.co.thomasc.lvf.TFL;

/**
 * Class for handling stop information new stops and stops with new names
 * get updated in the database
 */
public class Stops {
	
	/**
	 * Inner class encapsulating a single stop
	 */
	@EqualsAndHashCode
	@AllArgsConstructor
	private class Stop {
		private String stopName;
		private String lbsl;
	}
	
	/**
	 * Stop map containing stop information keyed on stopid
	 */
	private Map<String, Stop> stops = new HashMap<String, Stop>();
	
	/**
	 * Stops constructor, loads current database state
	 * @throws SQLException If there's a problem loading stops
	 */
	public Stops() throws SQLException {
		PreparedStatement stmt = Main.sql.query("SELECT * FROM stops");
		ResultSet r = stmt.getResultSet();
		while (r.next()) {
			stops.put(r.getString("stopId"), new Stop(r.getString("stopName"), r.getString("lbsl")));
		}
	}
	
	/**
	 * Update state from a prediction
	 * @param tfl A prediction row from TFL
	 */
	public void stop(TFL tfl) {
		// Ignore predictions with empty or invalid stop names
		if (tfl.getStopName().equalsIgnoreCase("NULL") || tfl.getStopName().equalsIgnoreCase("NONE") || tfl.getStopName().length() == 0) {
			return;
		}
		
		// Create a new stop object
		Stop newStop = new Stop(tfl.getStopName(), tfl.getPrediction().getStop());
		// Get the current stop object
		Stop curStop = stops.get(tfl.getStop());
		
		// Check if the stop has changed
		if (curStop == null || !newStop.equals(curStop)) {
			// Update memory cache
			stops.put(tfl.getStop(), newStop);
			
			// Function call does INSERT OR UPDATE, in future versions of postgres this will supported as actual SQL
			Main.sql.function("{call update_stops(?, ?, ?)}", new Object[] {tfl.getStop(), tfl.getPrediction().getStop(), tfl.getStopName()});
		}
	}
	
}