package uk.co.thomasc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class Main {
	
	public static void main(String[] args) {
		new Main();
	}
	
	public static Mongo mongo;
	private Stats stats = new Stats();
	
	public Main() {
		JsonParser parser = new JsonParser();
		
		JsonObject login = (JsonObject) parser.parse(new InputStreamReader(this.getClass().getResourceAsStream("/login.json")));
		mongo = new Mongo(login);
		
		DefaultHttpClient client = new DefaultHttpClient();
		Credentials defaultcreds = new UsernamePasswordCredentials(((JsonObject) login.get("tfl")).get("user").getAsString(), ((JsonObject) login.get("tfl")).get("pass").getAsString());
		client.getCredentialsProvider().setCredentials(new AuthScope("countdown.api.tfl.gov.uk", 80), defaultcreds);
		
		DBCursor c = mongo.find("lvf_history").sort(new BasicDBObject("vid", 1));
		while (c.hasNext()) {
			DBObject r = c.next();
			Bus.getFromVid((Integer) r.get("vid")).initHistory(r);;
		}
		
		while (true) {
			InputStream is = null;
			try {
				HttpGet httpget = new HttpGet("http://countdown.api.tfl.gov.uk/interfaces/ura/stream_V1?ReturnList=StopCode1,VisitNumber,LineId,LineName,DirectionId,destinationtext,VehicleId,RegistrationNumber,EstimatedTime");
				HttpResponse response = client.execute(httpget);
				is = response.getEntity().getContent();
				BufferedReader stream = new BufferedReader(new InputStreamReader(is));
				
				String inputLine;
				while ((inputLine = stream.readLine()) != null) {
					TFL tfl = new TFL(parser.parse(inputLine));
					stats.incRows();
					if (tfl.getType() == 1) {
						DBObject query = new BasicDBObject().append("vid", tfl.getVid()).append("stopid", tfl.getStop()).append("visit", tfl.getVisit()).append("destination", tfl.getDest());
						DBObject update = new BasicDBObject("$set", new BasicDBObject().append("route", tfl.getRoute()).append("line_id", tfl.getLineid()).append("prediction", tfl.getTime()).append("dirid", tfl.getDirid()));
						mongo.update("lvf_predictions", query, update, true, false, WriteConcern.UNACKNOWLEDGED);
						
						Bus.getFromVid(tfl.getVid()).newData(tfl);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (is != null) {
					try {
						is.close();
					} catch (IOException e) {}
				}
			}
		}
	}
}
