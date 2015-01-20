package uk.co.thomasc.lvf.bus;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import uk.co.thomasc.lvf.Main;

public class Stops {
	
	@EqualsAndHashCode
	@AllArgsConstructor
	private class Stop {
		private String stopName;
		private String lbsl;
	}
	
	private Map<String, Stop> stops = new HashMap<String, Stop>();
	
	public Stops() throws SQLException {
		PreparedStatement stmt = Main.sql.query("SELECT * FROM lvf_stops");
		ResultSet r = stmt.getResultSet();
		while (r.next()) {
			stops.put(r.getString("stopId"), new Stop(r.getString("stopName"), r.getString("lbsl")));
		}
	}
	
	public void stop(String stopid, String stopName, String lbsl) {
		if (stopName.equalsIgnoreCase("NULL") || stopName.equalsIgnoreCase("NONE") || stopName.length() == 0) {
			return;
		}
		
		Stop newStop = new Stop(stopName, lbsl);
		Stop curStop = stops.get(stopid);
		
		if (curStop == null || !newStop.equals(curStop)) {
			stops.put(stopid, newStop);
			Main.sql.update("REPLACE INTO lvf_stops (stopId, stopName, lbsl) VALUES (?, ?, ?)", new Object[] {stopid, stopName, lbsl});
		}
	}
	
}