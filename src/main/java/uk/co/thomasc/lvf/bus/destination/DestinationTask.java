package uk.co.thomasc.lvf.bus.destination;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import uk.co.thomasc.lvf.Main;

public class DestinationTask extends TimerTask {

	private static Map<DestinationKey, DestinationMap> destinations = new HashMap<DestinationKey, DestinationMap>();

	public static void incrementCount(String route, String lineid, int dirid, String destination) {
		synchronized (destinations) {
			final DestinationKey key = new DestinationKey(route, lineid, dirid);

			if (!destinations.containsKey(key)) {
				destinations.put(key, new DestinationMap());
			}

			if (!destinations.get(key).containsKey(destination)) {
				destinations.get(key).put(destination, new Destination(destination));
			}

			destinations.get(key).get(destination).incCount();
		}
	}

	private final Timer t = new Timer();

	public DestinationTask() {
		final int period = 24 * 60 * 60 * 1000; // 24 hours
		this.t.schedule(this, period, period);
	}

	public void finish() {
		this.t.cancel();
	}

	@Override
	public void run() {
		synchronized (destinations) {
			for (final DestinationKey key : destinations.keySet()) {
				final DestinationMap map = destinations.get(key);

				final Destination largest = map.getLargest();
				if (largest.equals(map.getTopDest())) {
					if (map.incCount() > 2) {
						Main.sql.update("REPLACE INTO lvf_destinations (route, lineid, direction, destination) VALUES (?, ?, ?, ?)", new Object[] {key.getRoute(), key.getLineid(), key.getDirid(), largest.getDestination()});
					}
				} else {
					map.setTopDest(largest);
				}

				destinations.get(key).resetCounts();
			}
		}
	}

}
