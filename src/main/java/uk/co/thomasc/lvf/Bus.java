package uk.co.thomasc.lvf;

import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class Bus {
	
	private static Map<Integer, Bus> singleton = new HashMap<Integer, Bus>();
	private static PriorityQueue<Bus> queue = new PriorityQueue<Bus>(11, new Comparator<Bus>() {
		public int compare(Bus o1, Bus o2) {
			return o1.getHead().before(o2.getHead()) ? -1 : 1;
		}
	});
	
	public static Bus getFromUvi(int uvi) {
		DBObject vehicle = Main.mongo.findOne("lvf_vehicles", new BasicDBObject("uvi", uvi), new BasicDBObject().append("cdreg", 1).append("keep", 1).append("vid", 1));
		if (vehicle != null && vehicle.containsField("vid")) {
			int vid = (Integer) vehicle.get("vid");
			if (!singleton.containsKey(vid)) {
				singleton.put(vid, new Bus(uvi, vehicle));
			}
			return getFromVid(vid);
		}
		return null;
	}

	public static Bus getFromVid(int vid) {
		if (!singleton.containsKey(vid)) {
			singleton.put(vid, new Bus(vid));
		}
		return singleton.get(vid);
	}
	
	public static void checkQueue() {
		Date now = new Date();
		while (!queue.isEmpty() && queue.peek().getHead().before(now)) {
			queue.poll().updateQueue();
		}
	}

	private int vid;
	private int uvi;
	private String reg;
	private boolean keep = false;
	private boolean exists = true;
	
	private Map<String, Map<String, Map<String, Map<String, Date>>>> history = new HashMap<String, Map<String, Map<String, Map<String, Date>>>>();
	private PriorityQueue<Prediction> predictions = new PriorityQueue<Prediction>(11, new Comparator<Prediction>() {
		public int compare(Prediction o1, Prediction o2) {
			return o1.getTime().before(o2.getTime()) ? -1 : 1;
		}
	});
	private Map<String, Prediction> pred_update = new HashMap<String, Prediction>();
	
	private Bus(int vid) {
		this.vid = vid;
		DBObject vehicle = Main.mongo.findOne("lvf_vehicles", new BasicDBObject("vid", vid), new BasicDBObject().append("cdreg", 1).append("keep", 1).append("uvi", 1));
		if (vehicle != null) {
			this.reg = (String) vehicle.get("cdreg");
			this.keep = (Boolean) vehicle.get("keep");
			this.uvi = (Integer) vehicle.get("uvi");
		} else {
			// Vehicle doesn't exist!
			exists = false;
		}
	}
	
	private Bus(int uvi, DBObject vehicle) {
		this.uvi = uvi;
		this.reg = (String) vehicle.get("cdreg");
		this.keep = (Boolean) vehicle.get("keep");
		this.vid = (Integer) vehicle.get("vid");
	}

	private int getNewVid(int guess) {
		while (Main.mongo.exists("lvf_vehicles", new BasicDBObject("uvi", guess))) {
			guess = Main.mongo.incCounter("uvi");
		}
		return guess;
	}
	
	public void newData(TFL tfl) {
		checkQueue();
		
		checkVehicle(tfl);
		
		BasicDBObject update = new BasicDBObject();
		
		if (!history.containsKey(tfl.getKeytime())) {
			history.put(tfl.getKeytime(), new HashMap<String, Map<String,Map<String,Date>>>());
		}
		if (!history.get(tfl.getKeytime()).containsKey(tfl.getRoute())) {
			history.get(tfl.getKeytime()).put(tfl.getRoute(), new HashMap<String, Map<String,Date>>());
		}
		if (!history.get(tfl.getKeytime()).get(tfl.getRoute()).containsKey(tfl.getLineid())) {
			history.get(tfl.getKeytime()).get(tfl.getRoute()).put(tfl.getLineid(), new HashMap<String, Date>());
		}
		
		if (!history.get(tfl.getKeytime()).get(tfl.getRoute()).get(tfl.getLineid()).containsKey("first_seen") || history.get(tfl.getKeytime()).get(tfl.getRoute()).get(tfl.getLineid()).get("first_seen").after(tfl.getTime())) {
			history.get(tfl.getKeytime()).get(tfl.getRoute()).get(tfl.getLineid()).put("first_seen", tfl.getTime());
			update.append("first_seen", tfl.getTime());
			update.append("route", tfl.getRoute());
		}
		if (!history.get(tfl.getKeytime()).get(tfl.getRoute()).get(tfl.getLineid()).containsKey("last_seen") || history.get(tfl.getKeytime()).get(tfl.getRoute()).get(tfl.getLineid()).get("last_seen").before(tfl.getTime())) {
			history.get(tfl.getKeytime()).get(tfl.getRoute()).get(tfl.getLineid()).put("last_seen", tfl.getTime());
			update.append("last_seen", tfl.getTime());
			update.append("route", tfl.getRoute());
		}
		
		if (!update.isEmpty()) {
			Calendar cal = new GregorianCalendar();
			cal.setTime(tfl.getTime());
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			Main.mongo.update("lvf_history", new BasicDBObject().append("vid", this.uvi).append("date", cal.getTime()).append("lineid", tfl.getLineid()), new BasicDBObject("$set", update), true, false, WriteConcern.UNACKNOWLEDGED);
		}
		
		Prediction pred;
		if (pred_update.containsKey(tfl.getStop())) {
			pred = pred_update.get(tfl.getStop());
			predictions.remove(pred);
			
			pred.setRoute(tfl.getRoute());
			pred.setLineid(tfl.getLineid());
			pred.setTime(tfl.getTime());
			pred.setDirid(tfl.getDirid());
			pred.setDest(tfl.getDest());
		} else {
			pred = new Prediction(tfl.getRoute(), tfl.getLineid(), tfl.getTime(), tfl.getStop(), tfl.getDirid(), tfl.getDest());
		}
		pred_update.put(tfl.getStop(), pred);
		predictions.offer(pred);
		
		updateQueue();
	}
	
	private void checkVehicle(TFL tfl) {
		if (!exists) {
			DBObject old = Main.mongo.findOne("lvf_vehicles", new BasicDBObject("cdreg", tfl.getReg()));
			
			if (old != null) {
				int uvi = getNewVid(vid);
				if ((Boolean) old.get("pre")) {
					// Pre-populated
					Main.mongo.debug("New registration already in database (1prepopuated), - Old Uvi = " + old.get("uvi") + ", New VehicleId = " + vid + ", reg = " + tfl.getReg() + ", fleetnumber = " + old.get("fnum"));
					Main.mongo.update("lvf_vehicles", new BasicDBObject("cdreg", tfl.getReg()), new BasicDBObject().append("$unset", new BasicDBObject("pre", 1)).append("$set", new BasicDBObject().append("vid", vid).append("uvi", uvi)));
					this.reg = tfl.getReg();
					this.uvi = uvi;
					this.keep = true;
				} else {
					// Registration changing vids
					preEntered(tfl, (Integer) old.get("uvi"), uvi);
				}
			} else {
				doInsert(tfl, getNewVid(vid));
			}
		} else if (tfl.getReg() != reg) {
			String tReg = tfl.getReg().replace(".", "");
			// Registration change
			if (!keep || tReg != reg) {
				DBObject old = Main.mongo.findAndModify("lvf_vehicles", new BasicDBObject("cdreg", reg), new BasicDBObject("$unset", new BasicDBObject("vid", 1)));
				Main.mongo.debug("Registration already exists in vehicle data - VehicleId = " + old.get("uvi") + ", reg = " + tfl.getReg() + ", old reg = " + this.reg + ", fleetnumber = " + old.get("fnum"));
				Main.mongo.update("lvf_vehicles", new BasicDBObject("vid", vid), new BasicDBObject("$set", new BasicDBObject("cdreg", tfl.getReg())));
				this.reg = tfl.getReg();
			} else {
				int uvi = getNewVid(vid);
				Main.mongo.update("lvf_vehicles", new BasicDBObject("uvi", this.uvi), new BasicDBObject("$unset", new BasicDBObject().append("vid", 1).append("cdreg", 1)));
				
				DBObject old = Main.mongo.findOne("lvf_vehicles", new BasicDBObject("cdreg", tfl.getReg()));
				if (old != null) {
					if ((Boolean) old.get("pre")) {
						// Pre-populated :D
						Main.mongo.debug("New registration already in database (2prepopuated), - Old Uvi = " + old.get("uvi") + ", New VehicleId = " + vid + ", reg = " + tfl.getReg() + ", fleetnumber = " + old.get("fnum"));
						Main.mongo.update("lvf_vehicles", new BasicDBObject("cdreg", tfl.getReg()), new BasicDBObject().append("$unset", new BasicDBObject("pre", 1)).append("$set", new BasicDBObject().append("vid", vid).append("uvi", uvi)));
						this.uvi = uvi;
					} else {
						preEntered(tfl, (Integer) old.get("uvi"), uvi);
					}
				} else {
					doInsert(tfl, uvi);
				}
			}
		}
	}

	private void preEntered(TFL tfl, int oldUvi, int newUvi) {
		Main.mongo.update("lvf_vehicles", new BasicDBObject("uvi", oldUvi), new BasicDBObject("$unset", new BasicDBObject().append("vid", 1).append("cdreg", 1).append("operator", 1).append("fnum", 1)));
		
		DBObject pre = Main.mongo.findOne("lvf_vehicles", new BasicDBObject().append("orig_reg", tfl.getReg()).append("pre", true));
		if (pre != null) {
			Main.mongo.debug("Found pre-entered data in vehicles table - Vid = " + vid + ", uvi = " + newUvi + ", reg = " + tfl.getReg() + ", operator = " + pre.get("operator") + ", fleetnumber = " + pre.get("fnum"));
			Main.mongo.update("lvf_vehicles", new BasicDBObject().append("orig_reg", tfl.getReg()).append("pre", true), new BasicDBObject().append("$unset", new BasicDBObject("pre", 1)).append("$set", new BasicDBObject().append("vid", vid).append("uvi", newUvi).append("keep", true).append("cdreg", tfl.getReg())));
			this.reg = tfl.getReg();
			this.keep = true;
			this.uvi = newUvi;
		} else {
			doInsert(tfl, getNewVid(vid));
		}
	}

	private void doInsert(TFL tfl, int uvi) {
		Stats.event("insert_veh");
		Main.mongo.debug("New entry in vehicles table - Vid = " + vid + " uvi = " + uvi + " Regns = " + tfl.getReg());
		Main.mongo.insert(
			"lvf_vehicles",
			new BasicDBObject()
				.append("cdreg", tfl.getReg())
				.append("vid", vid)
				.append("uvi", uvi)
				.append("cur_reg", tfl.getReg())
				.append("orig_reg", tfl.getReg())
				.append("keep", false)
				.append("operator", "UN")
				.append("lists", new BasicDBList().add("new"))
		);
		this.exists = true;
		this.uvi = uvi;
		this.reg = tfl.getReg();
		this.keep = false;
	}

	private Date getHead() {
		return predictions.peek().getTime();
	}
	
	private void updateQueue() {
		queue.remove(this);
		
		Date now = new Date();
		while (!predictions.isEmpty() && predictions.peek().getTime().before(now)) {
			Prediction pred = predictions.poll();
			pred_update.remove(pred.getStop());
		}
		
		if (!predictions.isEmpty()) {
			Main.mongo.update("lvf_vehicles", new BasicDBObject("vid", this.vid), new BasicDBObject("$set", new BasicDBObject("whereseen", predictions.peek().toDbObject())), false, false, WriteConcern.UNACKNOWLEDGED);
			
			queue.offer(this);
		}
	}

	public void initHistory(DBObject r) {
		Map<String, Date> history_a = new HashMap<String, Date>();
		history_a.put("last_seen", (Date) r.get("last_seen"));
		history_a.put("first_seen", (Date) r.get("first_seen"));
		
		Map<String, Map<String, Date>> history_b = new HashMap<String, Map<String,Date>>();
		history_b.put((String) r.get("lineid"), history_a);
		
		Map<String, Map<String, Map<String, Date>>> history_c = new HashMap<String, Map<String, Map<String,Date>>>();
		history_c.put((String) r.get("route"), history_b);
		
		history.put(TFL.dateFormat.format(r.get("date")), history_c);
	}
	
}