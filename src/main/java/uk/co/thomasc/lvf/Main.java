package uk.co.thomasc.lvf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.bson.types.ObjectId;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

import uk.co.thomasc.lvf.bus.Bus;
import uk.co.thomasc.lvf.task.Tasks;

public class Main {
	
	public static void main(String[] args) {
		new Main();
	}
	
	public static Mongo mongo;
	public static int backoff = 2500;
	private Stats stats = new Stats();
	private Tasks tasks = new Tasks();
	
	public Main() {
		JsonParser parser = new JsonParser();
		
		JsonObject login = (JsonObject) parser.parse(new InputStreamReader(this.getClass().getResourceAsStream("/login.json")));
		mongo = new Mongo(login);
		
		if (mongo.isMaster()) {
			DefaultHttpClient client = new DefaultHttpClient();
			Credentials defaultcreds = new UsernamePasswordCredentials(((JsonObject) login.get("tfl")).get("user").getAsString(), ((JsonObject) login.get("tfl")).get("pass").getAsString());
			client.getCredentialsProvider().setCredentials(new AuthScope("countdown.api.tfl.gov.uk", 80), defaultcreds);
			
			DBCursor c = mongo.find("lvf_vehicles", new BasicDBObject("vid", new BasicDBObject("$exists", true)), new BasicDBObject().append("cdreg", 1).append("uvi", 1).append("vid", 1));
			int loaded = 0;
			while (c.hasNext()) {
				DBObject r = c.next();
				loaded++;
				new Bus(r);
			}
			System.out.println("Loaded " + loaded + " vehicles");
			
			c = mongo.find("lvf_history", new BasicDBObject("date", new BasicDBObject("$gte", midnight()))).sort(new BasicDBObject("vid", 1));
			while (c.hasNext()) {
				DBObject r = c.next();
				Bus bus = Bus.getFromUvi((Integer) r.get("vid"));
				if (bus != null) {
					bus.initHistory(r);
				}
			}
			
			System.out.println("Finished Loading");
			
			while (true) {
				InputStream is = null;
				HttpGet httpget = null;
				Future<String> future = null;
				try {
					// Wait a bit before retrying
					if (backoff > 2500) {
						System.out.println("Connection error! Waiting " + (backoff / 1000) + " seconds before trying again");
					}
					Thread.sleep(backoff);
					if (backoff < 320000) {
						backoff *= 2;
					}
					
					httpget = new HttpGet("http://countdown.api.tfl.gov.uk/interfaces/ura/stream_V1?ReturnList=StopCode1,VisitNumber,LineId,LineName,DirectionId,destinationtext,VehicleId,RegistrationNumber,EstimatedTime");
					HttpResponse response = client.execute(httpget);
					is = response.getEntity().getContent();
					final BufferedReader stream = new BufferedReader(new InputStreamReader(is));
					String inputLine;
					Callable<String> readTask = new Callable<String>() {
						@Override
						public String call() throws Exception {
							return stream.readLine();
						}
					};
					ExecutorService executor = Executors.newFixedThreadPool(1);
					do {
						future = executor.submit(readTask);
						inputLine = future.get(30000, TimeUnit.MILLISECONDS);
						if (inputLine != null) {
							if (tasks.hasTasks()) {
								DBObject[] tsks = tasks.getTasks();
								for (DBObject task : tsks) {
									try {
										Bus bus = Bus.getFromUvi((Integer) task.get("uvi"));
										if (bus != null) {
											bus.performTask((String) task.get("task"));
										}
										tasks.completed((ObjectId) task.get("_id"));
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							}
							
							TFL tfl = new TFL(parser.parse(inputLine));
							stats.incRows();
							if (tfl.getType() == 1) {
								DBObject query = new BasicDBObject().append("vid", tfl.getVid()).append("stopid", tfl.getStop()).append("visit", tfl.getVisit()).append("destination", tfl.getDest());
								DBObject update = new BasicDBObject("$set", new BasicDBObject().append("route", tfl.getRoute()).append("line_id", tfl.getLineid()).append("prediction", tfl.getTime()).append("dirid", tfl.getDirid()));
								mongo.update("lvf_predictions", query, update, true, false, WriteConcern.UNACKNOWLEDGED);
								
								Bus.getFromVid(tfl.getVid()).newData(tfl);
							}
						}
					} while (inputLine != null);
				} catch (Exception e) {
					e.printStackTrace();
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
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		stats.finish();
		tasks.finish();
	}
	
	public static Date midnight() {
		return midnight(new Date());
	}

	public static Date midnight(Date date) {
		Calendar cal = new GregorianCalendar();
		cal.setTime(date);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}
}
