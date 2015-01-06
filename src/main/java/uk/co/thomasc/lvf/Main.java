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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import uk.co.thomasc.lvf.bus.Bus;
import uk.co.thomasc.lvf.bus.destination.DestinationTask;
import uk.co.thomasc.lvf.task.Tasks;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Main {

	public static void main(String[] args) throws Exception {
		new Main();
	}

	public static Logger logger = Logger.getLogger("LVF Main");
	public static Sql sql;
	public static int backoff = 2500;
	private final Stats stats = new Stats();
	private Tasks tasks;

	public Main() throws Exception {
		logger.setLevel(Level.ALL);

		final JsonParser parser = new JsonParser();

		// Read username-password from login.json
		final JsonObject login = (JsonObject) parser.parse(new InputStreamReader(this.getClass().getResourceAsStream("/login.json")));
		sql = new Sql((JsonObject) login.get("sql")); // login to database
		this.tasks = new Tasks(); // set task handler
		new DestinationTask();

		final DefaultHttpClient client = new DefaultHttpClient();
		final Credentials defaultcreds = new UsernamePasswordCredentials(((JsonObject) login.get("tfl")).get("user").getAsString(), ((JsonObject) login.get("tfl")).get("pass").getAsString());
		client.getCredentialsProvider().setCredentials(new AuthScope("countdown.api.tfl.gov.uk", 80), defaultcreds);

		// read vehicles table, that are active (have vids), read cdreg, uvi & vid
		PreparedStatement stmt = sql.query("SELECT cdreg, uvi, vid FROM lvf_vehicles WHERE vid IS NOT NULL");
		ResultSet c = stmt.getResultSet();
		int loaded = 0;
		while (c.next()) {
			loaded++;
			new Bus(c);
		}
		logger.log(Level.INFO, "Loaded " + loaded + " vehicles");
		stmt.close();

		// load current day history records for active vehicles
		stmt = sql.query("SELECT * FROM lvf_route_day WHERE date = CURDATE() ORDER BY vid ASC");
		c = stmt.getResultSet();
		while (c.next()) {
			final Bus bus = Bus.getFromUvi(c.getInt("vid"));
			if (bus != null) {
				bus.initHistory(c);
			}
		}
		stmt.close();

		logger.log(Level.INFO, "Finished Loading");

		while (true) {
			InputStream is = null;
			HttpGet httpget = null;
			Future<String> future = null;
			try {
				// Wait a bit before retrying
				if (backoff > 2500) {
					logger.log(Level.INFO, "Connection error! Waiting " + backoff / 1000 + " seconds before trying again");
				}
				Thread.sleep(backoff);
				if (backoff < 320000) {
					backoff *= 2;
				}

				// open TFL connection....
				httpget = new HttpGet("http://countdown.api.tfl.gov.uk/interfaces/ura/stream_V1?ReturnList=StopCode1,VisitNumber,LineId,LineName,DirectionId,destinationtext,VehicleId,RegistrationNumber,EstimatedTime,ExpireTime");
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
					future = executor.submit(readTask);
					inputLine = future.get(30000, TimeUnit.MILLISECONDS);
					if (inputLine != null) {
						// here when line read from TFL
						if (this.tasks.hasTasks()) {
							final JsonObject[] tsks = this.tasks.getTasks();
							for (final JsonObject task : tsks) {
								try {
									final Bus bus = Bus.getFromUvi(task.get("uvi").getAsInt());
									if (bus != null) {
										bus.performTask(task.get("task").getAsString(), (JsonObject) task.get("extra"));
									}
									this.tasks.completed(c.getInt("id"));
								} catch (final Exception e) {
									this.tasks.failed(c.getInt("id"), e);
								}
							}
						}

						// here to process TFL lines
						final TFL tfl = new TFL(parser.parse(inputLine));
						this.stats.incRows();
						if (tfl.getType() == 1) {
							try {
								if (Bus.getFromVid(tfl.getVid()).newData(tfl)) {
									this.stats.incInteresting();
									sql.update("REPLACE INTO lvf_predictions " + "(vid, stopid, visit, destination, route, line_id, prediction, dirid, valid) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", tfl.toDbObject());
								}
							} catch (final Exception e) {
								Main.logger.log(Level.SEVERE, "Error processing row: " + tfl, e);
								throw e;
							}
						}
					}
				} while (inputLine != null);
			} catch (final Exception e) {
				Main.logger.log(Level.SEVERE, "Error in outer catch. Bad.", e);
			} finally {
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
						Main.logger.log(Level.WARNING, "Error closing input stream", e);
					}
				}
			}
		}

		// stats.finish(); Unreachable, which is good
		// tasks.finish();
		// destTask.finish();
	}
}
