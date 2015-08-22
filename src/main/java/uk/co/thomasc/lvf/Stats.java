package uk.co.thomasc.lvf;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Deals with statistics
 * Tracks number of database updates per second and 'events'
 * An event can be any string that you want to know the frequency of
 */
public class Stats extends Thread {
	
	/**
	 * Used to stop the thread when needed
	 */
	private boolean running = true;
	/**
	 * Total prediction counter
	 */
	private int rows = 0;
	/**
	 * Total interesting prediction counter
	 */
	private int interesting = 0;
	/**
	 * Thread loop counter, used to do things less often
	 */
	private int i = 0;
	/**
	 * Event counter map
	 */
	private static Map<String, AtomicInteger> stats = new HashMap<String, AtomicInteger>();

	/**
	 * Stats constructor, automatically starts the thread
	 */
	public Stats() {
		this.start();
	}

	/**
	 * Increments the prediction row counter
	 */
	public void incRows() {
		this.rows++;
	}

	/**
	 * Increments the interesting prediction row counter
	 */
	public void incInteresting() {
		this.interesting++;
	}

	/**
	 * Main stats loop
	 */
	@Override
	public void run() {
		while (this.running) {
			try {
				// Wait one second between updates
				sleep(1000);

				if (this.rows > 0) {
					Main.logger.log(Level.FINE, "New data in " + this.interesting + "/" + this.rows + " rows");
					this.rows = 0;
					this.interesting = 0;
					Main.backoff = 5000;
				}

				// Do this section only 1/60 times the thread loops
				if (++this.i % 60 == 0) {
					// Reset i, not strictly needed
					this.i = 0;
					
					// Loop through events
					for (final Entry<String, AtomicInteger> entry : stats.entrySet()) {
						// Reset counter and get its old value
						final int val = entry.getValue().getAndSet(0);
						
						// If any instances of the event happened run an update
						if (val > 0) {
							// Function call does INSERT OR UPDATE, in future versions of postgres this will supported as actual SQL
							Main.sql.function("{call update_stats(?, ?)}", new Object[] {entry.getKey(), val});
						}
					}
				}
			} catch (final InterruptedException e) {
				// Other threads can interupt this thread to force
				// instant end of execution (if finish is also called)
				Main.logger.log(Level.WARNING, "Stats task interrupted", e);
			}
		}
	}

	/**
	 * Stops the thread looping
	 */
	public void finish() {
		this.running = false;
	}

	/**
	 * Increments the counter for an event
	 * @param key The event name
	 */
	public static void event(String key) {
		if (stats.containsKey(key)) {
			stats.get(key).incrementAndGet();
		} else {
			stats.put(key, new AtomicInteger(1));
		}
	}

}
