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
		this.start();
	}

	public void incRows() {
		this.rows++;
	}

	public void incInteresting() {
		this.interesting++;
	}

	@Override
	public void run() {
		while (this.running) {
			try {
				sleep(1000);

				if (this.rows > 0) {
					Main.logger.log(Level.INFO, "Rows: " + this.interesting + "/" + this.rows + "\r");
					this.rows = 0;
					this.interesting = 0;
					Main.backoff = 5000;
				}

				if (++this.i % 60 == 0) {
					this.i = 0;
					for (final Entry<String, AtomicInteger> entry : stats.entrySet()) {
						final int val = entry.getValue().getAndSet(0);
						if (val > 0) {
							Main.sql.update("INSERT INTO lvf_stats (stat, date, count) VALUES (?, CURDATE(), ?) ON DUPLICATE KEY UPDATE count = count + ?", new Object[] {entry.getKey(), val, val});
						}
					}
				}
			} catch (final InterruptedException e) {
				Main.logger.log(Level.WARNING, "Stats task interrupted", e);
			}
		}
	}

	public void finish() {
		this.running = false;
	}

	public static void event(String key) {
		if (stats.containsKey(key)) {
			stats.get(key).incrementAndGet();
		} else {
			stats.put(key, new AtomicInteger(1));
		}
	}

}
