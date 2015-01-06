package uk.co.thomasc.lvf;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class Stats extends Thread {
	
	private boolean running = true;
	private int rows = 0;
	private int interesting = 0;
	private int i = 0;
	private static Map<String, AtomicInteger> stats = new HashMap<String, AtomicInteger>();
	
	public Stats() {
		start();
	}
	
	public void incRows() {
		rows++;
	}
	
	public void incInteresting() {
		interesting++;
	}
	
	@Override
	public void run() {
		while (running) {
			try {
				sleep(1000);
				
				if (rows > 0) {
					Main.logger.log(Level.INFO, "Rows: " + interesting + "/" + rows + "\r");
					rows = 0;
					interesting = 0;
					Main.backoff = 5000;
				}
				
				if (++i % 60 == 0) {
					i = 0;
					for (Entry<String, AtomicInteger> entry : stats.entrySet()) {
						int val = entry.getValue().getAndSet(0);
						if (val > 0) {
							Main.sql.update("INSERT INTO lvf_stats (stat, date, count) VALUES (?, CURDATE(), ?) ON DUPLICATE KEY UPDATE count = count + ?", new Object[] {entry.getKey(), val, val});
						}
					}
				}
			} catch (InterruptedException e) {
				Main.logger.log(Level.WARNING, "Stats task interrupted", e);
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