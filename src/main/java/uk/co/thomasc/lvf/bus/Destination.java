package uk.co.thomasc.lvf.bus;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.mongodb.BasicDBObject;
import com.mongodb.WriteConcern;

import uk.co.thomasc.lvf.Main;

@ToString
public class Destination {

	@Getter private static Map<String, Map<String, Map<Integer, Map<String, Destination>>>> destinations = new HashMap<String, Map<String, Map<Integer, Map<String, Destination>>>>();
	
	public static void incrementCount(String route, String lineid, int dirid, String destination) {
		synchronized (destinations) {
			if (!destinations.containsKey(route)) {
				destinations.put(route, new HashMap<String, Map<Integer, Map<String, Destination>>>());
			}
			if (!destinations.get(route).containsKey(lineid)) {
				destinations.get(route).put(lineid, new HashMap<Integer, Map<String, Destination>>());
			}
			if (!destinations.get(route).get(lineid).containsKey(dirid)) {
				destinations.get(route).get(lineid).put(dirid, new HashMap<String, Destination>());
			}
			if (!destinations.get(route).get(lineid).get(dirid).containsKey(destination)) {
				Main.mongo.update("lvf_destinations", new BasicDBObject().append("route", route).append("lineid", lineid).append("direction", dirid).append("destination", destination), new BasicDBObject().append("$setOnInsert", new BasicDBObject("day", "SD").append("dest_cnt", 0)), true, false, WriteConcern.UNACKNOWLEDGED);
				destinations.get(route).get(lineid).get(dirid).put(destination, new Destination(destination));
			}
			
			destinations.get(route).get(lineid).get(dirid).get(destination).inc();
		}
	}
	
	@Getter @Setter private int count = 0;
	@Getter private String destination;
	
	public Destination(String destination) {
		this.destination = destination;
	}
	
	private void inc() {
		count++;
	}
	
}
