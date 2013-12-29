package uk.co.thomasc.lvf.bus;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import uk.co.thomasc.lvf.Main;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DestinationTask extends TimerTask {

	private Timer t = new Timer();
	
	public DestinationTask() {
		int period = 1 * 60 * 60 * 1000;
		t.schedule(this, period, period);
	}
	
	public void finish() {
		t.cancel();
	}

	@Override
	public void run() {
		Map<String, Map<String, Map<Integer, Map<String, Destination>>>> destMap = Destination.getDestinations();
		synchronized (destMap) {
			for (String route : destMap.keySet()) {
				for (String lineid : destMap.get(route).keySet()) {
					for (int dirid : destMap.get(route).get(lineid).keySet()) {
						Destination largest = null;
						boolean seenReal = false;
						
						for (String destination : destMap.get(route).get(lineid).get(dirid).keySet()) {
							Destination current = destMap.get(route).get(lineid).get(dirid).get(destination);
							if (largest == null || current.getCount() > largest.getCount()) {
								if (largest != null) {
									seenReal |= resetCount(route, lineid, dirid, destination, current.getCount());
								}
								largest = current;
							} else {
								seenReal |= resetCount(route, lineid, dirid, destination, current.getCount());
							}
							current.setCount(0);
						}
						// If largest is SD and we haven't seen the real one, inc
						if (!seenReal) {
							DBObject obj = Main.mongo.findAndModify("lvf_destinations", new BasicDBObject().append("route", route).append("lineid", lineid).append("direction", dirid).append("destination", largest.getDestination()), new BasicDBObject("largest", 1), new BasicDBObject().append("$set", new BasicDBObject("dest_cnt", largest.getCount())).append("$inc", new BasicDBObject("largest", 1)), true, true);
							if ((Integer) obj.get("largest") > 2) {
								Main.mongo.debug("New destination? Route: " + route + ", LineID: " + lineid + ", Dir: " + dirid + ", " + largest);
							}
						} else {
							resetCount(route, lineid, dirid, largest.getDestination(), largest.getCount());
						}
					}
				}
			}
		}
	}
	
	private static boolean resetCount(String route, String lineid, int dirid, String destination, int count) {
		DBObject result = Main.mongo.findAndModify("lvf_destinations", new BasicDBObject().append("route", route).append("lineid", lineid).append("direction", dirid).append("destination", destination), new BasicDBObject("day", 1), new BasicDBObject().append("$setOnInsert", new BasicDBObject("day", "SD")).append("$set", new BasicDBObject("dest_cnt", count).append("largest", 0)), false, true);
		return ((String) result.get("day")).isEmpty();
	}
	
}
