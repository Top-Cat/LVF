package uk.co.thomasc.lvf;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;

public class Stats extends Thread {
	
	private boolean running = true;
	private int rows = 0;
	private int i = 0;
	private static Map<String, AtomicInteger> stats = new HashMap<String, AtomicInteger>();
	
	public Stats() {
		start();
	}
	
	public void incRows() {
		rows++;
	}
	
	@Override
	public void run() {
		while (running) {
			try {
				sleep(1000);
				
				if (rows > 0) {
					System.out.println("Rows: " + rows + "\r");
					rows = 0;
				}
				
				if (++i % 60 == 0) {
					i = 0;
					BasicDBObject statsUpdate = new BasicDBObject();
					for (Entry<String, AtomicInteger> entry : stats.entrySet()) {
						int val = entry.getValue().getAndSet(0);
						if (val > 0) {
							statsUpdate.append(entry.getKey(), entry.getValue().getAndSet(0));
						}
					}
					if (!statsUpdate.isEmpty()) {
						Main.mongo.update(
							"lvf_stats",
							new BasicDBObject("date", Main.midnight()),
							new BasicDBObject("$inc", statsUpdate),
							true
						);
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void finish() {
		running = false;
	}

	public static void event(String key) {
		if (stats.containsKey(key)) {
			stats.get(key).incrementAndGet();
		} else {
			stats.put(key, new AtomicInteger(1));
		}
	}
	
}