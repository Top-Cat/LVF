package uk.co.thomasc.lvf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import uk.co.thomasc.lvf.bus.Bus;
import uk.co.thomasc.lvf.bus.Stops;
import uk.co.thomasc.lvf.bus.destination.DestinationTask;
import uk.co.thomasc.lvf.network.TaskServer;
import uk.co.thomasc.lvf.packets.TaskPacket;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Main class containing the main loop
 * Initialises worker threads and then reads TFL data until killed
 */
public class Main {

	/**
	 * Program entry point
	 * Makes an instance of the Main class
	 * @param args Arguments passed on the command line
	 */
	public static void main(String[] args) {
		new Main();
	}

	/**
	 * Main logger for giving feedback on the console and to disk
	 */
	public static Logger logger = Logger.getLogger("LVF Main");
	/**
	 * SQL abstraction helper
	 */
	public static Sql sql;
	/**
	 * Task server helper
	 */
	public static TaskServer tasks;
	/**
	 * Stop name helper
	 */
	public static Stops stops;
	/**
	 * Destination task
	 */
	public static DestinationTask destTask;
	/**
	 * How long to wait before next http connection attempt
	 */
	public static int backoff = 2500;
	/**
	 * Stats helper
	 */
	private final Stats stats = new Stats();

	/**
	 * Main Program Loop
	 */
	public Main() {
		try {
			// Set up logging, console will get INFO and up,
			// all messages will be logged to file
			logger.setUseParentHandlers(false);
			logger.setLevel(Level.ALL);
			
			ConsoleHandler ch = new ConsoleHandler();
			ch.setLevel(Level.INFO);
			logger.addHandler(ch);
			
			FileHandler fh = new FileHandler("./lvf.log", true);
			fh.setFormatter(new LogFormatter());
			fh.setLevel(Level.ALL);
			logger.addHandler(fh);
	
			// Initialise parser, TFL sends updates in JSON format
			final JsonParser parser = new JsonParser();
	
			// Read credentials from login.json
			final JsonObject login = (JsonObject) parser.parse(new InputStreamReader(this.getClass().getResourceAsStream("/login.json")));
			
			// Initialise helper classes / threads
			sql = new Sql((JsonObject) login.get("sql")); // login to database
			stops = new Stops(); // Stops helper
			tasks = new TaskServer(); // Allows the website to send actions
			destTask = new DestinationTask(); // Destination helper
	
			// Initialise HTTP connection
			final DefaultHttpClient client = new DefaultHttpClient();
			final Credentials defaultcreds = new UsernamePasswordCredentials(((JsonObject) login.get("tfl")).get("user").getAsString(), ((JsonObject) login.get("tfl")).get("pass").getAsString());
			client.getCredentialsProvider().setCredentials(new AuthScope("countdown.api.tfl.gov.uk", 80), defaultcreds);
	
			// Read vehicles table, that are active (have vids),
			// read cdreg, uvi & vid
			// This speeds up startup where active vehicles would get loaded
			// one at a time as they are referenced by updates
			PreparedStatement stmt = sql.query("SELECT cdreg, uvi, vid FROM vehicles WHERE vid IS NOT NULL");
			ResultSet c = stmt.getResultSet();
			int loaded = 0;
			while (c.next()) {
				loaded++;
				new Bus(c);
			}
			logger.log(Level.INFO, "Loaded " + loaded + " vehicles");
			stmt.close();
	
			// load current day history records for active vehicles
			stmt = sql.query("SELECT * FROM route_day WHERE date = current_date ORDER BY vid ASC");
			c = stmt.getResultSet();
			while (c.next()) {
				final Bus bus = Bus.getFromUvi(c.getInt("vid"));
				if (bus != null) {
					bus.initHistory(c);
				}
			}
			stmt.close();
	
			logger.log(Level.INFO, "Finished Loading");
	
			// Start main listening loop, loops when the connection to TFL
			// is lost waiting between attempts as per spec
			while (true) {
				InputStream is = null;
				HttpGet httpget = null;
				Future<String> future = null;
				try {
					// Wait a bit before retrying,
					// increases on subsequent failures without any valid data
					if (backoff > 2500) {
						logger.log(Level.INFO, "Connection error! Waiting " + backoff / 1000 + " seconds before trying again");
					}
					Thread.sleep(backoff);
					if (backoff < 320000) {
						backoff *= 2;
					}

					// open TFL connection....
					httpget = new HttpGet("http://countdown.api.tfl.gov.uk/interfaces/ura/stream_V1?ReturnList=StopPointName,StopID,StopCode1,VisitNumber,LineId,LineName,DirectionId,destinationtext,VehicleId,RegistrationNumber,EstimatedTime,ExpireTime");
					final HttpResponse response = client.execute(httpget);
					is = response.getEntity().getContent();
					final BufferedReader stream = new BufferedReader(new InputStreamReader(is));
					String inputLine;
					final Callable<String> readTask = new Callable<String>() {
						@Override
						public String call() throws Exception {
							return stream.readLine();
						}
					};
					final ExecutorService executor = Executors.newFixedThreadPool(1);
					do {
						// Call readLine on another thread and wait for
						// up to 30 seconds for a response before giving up
						future = executor.submit(readTask);
						inputLine = future.get(30000, TimeUnit.MILLISECONDS);
						
						// Do we have data?
						if (inputLine != null) {
							// here when line read from TFL
							
							// Do we have a waiting task to perform?
							TaskPacket task = tasks.getTaskQueue().poll();
							if (task != null) {
								try {
									final Bus bus = Bus.getFromUvi(task.getUvi());
									if (bus != null) {
										// Try to perform task
										bus.performTask(task);
									}
									task.setSuccess(true);
								} catch (Exception e) {
									e.printStackTrace();
								} finally {
									synchronized (task) {
										task.notify();
									}
								}
							}
	
							// Create a TFL object from the string returned from TFL
							final TFL tfl = new TFL(parser.parse(inputLine));
							
							// Increase total row counter
							this.stats.incRows();
							
							// Check it's a prediction (1)
							if (tfl.getType() == 1) {
								try {
									// Update the stop info for the stop in the prediction
									stops.stop(tfl);
									
									// Give the bus the data and if it's new enter the if 
									if (Bus.getFromVid(tfl.getVid()).newData(tfl)) {
										// Increase interesting row counter
										this.stats.incInteresting();
										
										// Function call does INSERT OR UPDATE, in future versions of postgres this will supported as actual SQL
										sql.function("{call update_predictions (?, ?, ?::timestamp, ?::smallint, ?, ?, ?, ?::smallint, ?)}", tfl.toDbObject());
									}
								} catch (final Exception e) {
									logger.log(Level.SEVERE, "Error processing row: " + tfl, e);
									throw e;
								}
							}
						}
					} while (inputLine != null);
				} catch (final Exception e) {
					logger.log(Level.SEVERE, "Error in outer catch. Bad.", e);
				} finally {
					// Don't leak memory, always safely destory connections
					if (httpget != null) {
						httpget.reset();
					}
					if (future != null) {
						future.cancel(true);
					}
					if (is != null) {
						try {
							is.close();
						} catch (final IOException e) {
							logger.log(Level.WARNING, "Error closing input stream", e);
						}
					}
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Error initialising server.", e);
		} finally {
			stats.finish(); //Unreachable, which is good
			tasks.finish();
			destTask.finish();
		}
	}
}
