package uk.co.thomasc.lvf.bus;

import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

import uk.co.thomasc.lvf.Main;
import uk.co.thomasc.lvf.Stats;
import uk.co.thomasc.lvf.TFL;

public class Bus {
	
	private static Random rand = new Random();
	private static Map<Integer, Bus> singleton = new HashMap<Integer, Bus>();
	private static Map<Integer, Bus> singletonUvi = new HashMap<Integer, Bus>();
	private static PriorityQueue<Bus> queue = new PriorityQueue<Bus>(11, new Comparator<Bus>() {
		public int compare(Bus o1, Bus o2) {
			return o1.getHead().before(o2.getHead()) ? -1 : 1;
		}
	});
	
	public static Bus getFromUvi(int uvi) {
		if (!singletonUvi.containsKey(uvi)) {
			DBObject r = Main.mongo.findOne("lvf_vehicles", new BasicDBObject("uvi", uvi), new BasicDBObject().append("cdreg", 1).append("uvi", 1).append("vid", 1));
			if (r != null) {
				new Bus(r);
			}
		}
		return singletonUvi.get(uvi);
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
		DBObject vehicle = Main.mongo.findOne("lvf_vehicles", new BasicDBObject("vid", vid), new BasicDBObject().append("cdreg", 1).append("uvi", 1));
		if (vehicle != null) {
			this.reg = (String) vehicle.get("cdreg");
			this.uvi = (Integer) vehicle.get("uvi");
		} else {
			// Vehicle doesn't exist!
			exists = false;
		}
	}

	public Bus(DBObject vehicle) {
		this.reg = (String) vehicle.get("cdreg");
		this.uvi = (Integer) vehicle.get("uvi");
		singletonUvi.put(uvi, this);
		if (vehicle.containsField("vid")) {
			this.vid = (Integer) vehicle.get("vid");
			if (!singleton.containsKey(vid)) {
				singleton.put(vid, this);
			}
		}
	}

	private int getNewVid(int guess) {
		int inc = 0;
		while (Main.mongo.exists("lvf_vehicles", new BasicDBObject("uvi", guess))) {
			guess = Main.mongo.incCounter("uvi", inc);
			inc = 1;
		}
		return guess;
	}
	
	public boolean newData(TFL tfl) {
		checkQueue();
		
		checkVehicle(tfl);
		
		Prediction pred;
		if (pred_update.containsKey(tfl.getStop())) {
			pred = pred_update.get(tfl.getStop());
			if (pred.getDifftime() == tfl.getDifftime() && pred.isValid() == tfl.isValid()) {
				return false;
			}
			predictions.remove(pred);
			
			pred.setRoute(tfl.getRoute());
			pred.setLineid(tfl.getLineid());
			pred.setTime(tfl.getTime());
			pred.setValid(tfl.isValid());
			pred.setKeytime(tfl.getKeytime());
			pred.setDirid(tfl.getDirid());
			pred.setDest(tfl.getDest());
		} else {
			pred = new Prediction(tfl.getRoute(), tfl.getLineid(), tfl.getTime(), tfl.isValid(), tfl.getKeytime(), tfl.getDifftime(), tfl.getStop(), tfl.getDirid(), tfl.getDest());
		}
		pred_update.put(tfl.getStop(), pred);
		predictions.offer(pred);
		
		updateQueue();
		return true;
	}
	
	private void updateHistory(String date, Date time, String route, String lineid) {
		BasicDBObject update = new BasicDBObject();
		
		Calendar cal = new GregorianCalendar();
		cal.setTime(time);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date today = cal.getTime();
		
		if (!history.containsKey(date)) {
			history.put(date, new HashMap<String, Map<String,Map<String,Date>>>());
		}
		if (!history.get(date).containsKey(route)) {
			history.get(date).put(route, new HashMap<String, Map<String,Date>>());
		}
		if (!history.get(date).get(route).containsKey(lineid)) {
			history.get(date).get(route).put(lineid, new HashMap<String, Date>());
			DBObject row = Main.mongo.findOne("lvf_history", new BasicDBObject().append("vid", uvi).append("date", today).append("route", route).append("lineid", lineid));
			if (row != null) {
				history.get(date).get(route).get(lineid).put("first_seen", (Date) row.get("first_seen"));
				history.get(date).get(route).get(lineid).put("last_seen", (Date) row.get("last_seen"));
			}
		}
		
		if (!history.get(date).get(route).get(lineid).containsKey("first_seen") || history.get(date).get(route).get(lineid).get("first_seen").after(time)) {
			history.get(date).get(route).get(lineid).put("first_seen", time);
			update.append("first_seen", time);
			update.append("route", route);
		}
		if (!history.get(date).get(route).get(lineid).containsKey("last_seen") || history.get(date).get(route).get(lineid).get("last_seen").before(time)) {
			history.get(date).get(route).get(lineid).put("last_seen", time);
			update.append("last_seen", time);
			update.append("route", route);
		}
		
		if (!update.isEmpty()) {
			Main.mongo.update("lvf_history", new BasicDBObject().append("vid", this.uvi).append("date", today).append("lineid", lineid), new BasicDBObject("$set", update), true, false, WriteConcern.UNACKNOWLEDGED);
		}
	}

	private void checkVehicle(TFL tfl) {
		if (!exists) {
			DBObject old = Main.mongo.findOne("lvf_vehicles", new BasicDBObject("cur_reg", tfl.getReg()));
			
			if (old != null) {
				int uvi = getNewVid(vid);
				if (old.containsField("pre") && (Boolean) old.get("pre")) {
					// Pre-populated
					Main.mongo.debug("1Prepopulated Vehicle (" + old.get("cur_reg") + ")", uvi);
					Main.mongo.update("lvf_vehicles", new BasicDBObject("cur_reg", tfl.getReg()), new BasicDBObject().append("$unset", new BasicDBObject("pre", 1)).append("$set", new BasicDBObject().append("vid", vid).append("uvi", uvi).append("cdreg", tfl.getReg())));
					this.exists = true;
					this.reg = tfl.getReg();
					this.uvi = uvi;
				} else {
					// Registration changing vids
					preEntered(tfl, (Integer) old.get("uvi"), uvi);
				}
			} else {
				doInsert(tfl, getNewVid(vid));
			}
		} else if (!tfl.getReg().equals(reg)) {
			boolean hasDot = tfl.getReg().contains(".");
			DBObject vehicle = Main.mongo.findOne("lvf_vehicles", new BasicDBObject("vid", vid), new BasicDBObject().append("keep", 1));
			// Registration change
			if (!((Boolean) vehicle.get("keep")) || hasDot) {
				DBObject old = Main.mongo.findAndModify("lvf_vehicles", new BasicDBObject("cdreg", tfl.getReg()), new BasicDBObject("$unset", new BasicDBObject("vid", 1).append("cdreg", 1)));
				if (old != null) {
					getFromVid((Integer) old.get("vid")).forceWithdraw();
				}
				
				Main.mongo.debug("Changed Vid (" + reg + ")", uvi);
				Main.mongo.update("lvf_vehicles", new BasicDBObject("vid", vid), new BasicDBObject("$set", new BasicDBObject("cdreg", tfl.getReg())));
				this.reg = tfl.getReg();
			} else {
				int uvi = getNewVid(vid);
				Main.mongo.update("lvf_vehicles", new BasicDBObject("uvi", this.uvi), new BasicDBObject("$unset", new BasicDBObject().append("vid", 1).append("cdreg", 1)));
				
				DBObject old = Main.mongo.findOne("lvf_vehicles", new BasicDBObject("cur_reg", tfl.getReg()));
				if (old != null) {
					if (old.containsField("pre") && (Boolean) old.get("pre")) {
						// Pre-populated :D
						Main.mongo.debug("2Prepopulated Vehicle (" + old.get("cur_reg") + ")", uvi);
						Main.mongo.update("lvf_vehicles", new BasicDBObject("cur_reg", tfl.getReg()), new BasicDBObject().append("$unset", new BasicDBObject("pre", 1)).append("$set", new BasicDBObject().append("vid", vid).append("uvi", uvi).append("cdreg", tfl.getReg())));
						this.uvi = uvi;
						this.reg = tfl.getReg();
					} else {
						preEntered(tfl, (Integer) old.get("uvi"), uvi);
					}
				} else {
					doInsert(tfl, uvi);
				}
			}
		}
	}

	private void forceWithdraw() {
		singleton.remove(vid);
		this.exists = false;
		this.reg = "";
		this.vid = 0;
	}

	private void preEntered(TFL tfl, int oldUvi, int newUvi) {
		Main.mongo.update("lvf_vehicles", new BasicDBObject("uvi", oldUvi), new BasicDBObject("$unset", new BasicDBObject().append("vid", 1).append("cdreg", 1).append("operator", 1).append("fnum", 1)));
		
		DBObject pre = Main.mongo.findOne("lvf_vehicles", new BasicDBObject().append("orig_reg", tfl.getReg()).append("pre", true));
		if (pre != null) {
			Main.mongo.debug("3Prepopulated Vehicle (" + pre.get("cur_reg") + ")", newUvi);
			Main.mongo.update("lvf_vehicles", new BasicDBObject().append("orig_reg", tfl.getReg()).append("pre", true), new BasicDBObject().append("$unset", new BasicDBObject("pre", 1)).append("$set", new BasicDBObject().append("vid", vid).append("uvi", newUvi).append("keep", true).append("cdreg", tfl.getReg())));
			this.exists = true;
			this.reg = tfl.getReg();
			this.uvi = newUvi;
		} else {
			doInsert(tfl, getNewVid(vid));
		}
	}

	private void doInsert(TFL tfl, int uvi) {
		Stats.event("insert_veh");
		Main.mongo.debug("New vehicle " + uvi + " (" + tfl.getReg() + ")", uvi);
		BasicDBList newlist = new BasicDBList();
		newlist.add("new");
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
				.append("lists", newlist)
		);
		this.exists = true;
		this.uvi = uvi;
		this.reg = tfl.getReg();
	}

	private Date getHead() {
		return predictions.peek().getTime();
	}
	
	private void updateQueue() {
		queue.remove(this);
		
		Date now = new Date();
		while (!predictions.isEmpty() && ( predictions.peek().getTime().before(now) || !predictions.peek().isValid())) {
			Prediction pred = predictions.poll();
			pred_update.remove(pred.getStop());
		}
		
		if (!predictions.isEmpty()) {
			Prediction pred = predictions.peek();
			updateHistory(pred.getKeytime(), pred.getTime(), pred.getRoute(), pred.getLineid());
			Main.mongo.update("lvf_vehicles", new BasicDBObject("vid", this.vid), new BasicDBObject("$set", new BasicDBObject("whereseen", pred.toDbObject())), false, false, WriteConcern.UNACKNOWLEDGED);
			if (rand.nextInt(100) == 0) {
				Main.mongo.update("lvf_destinations", new BasicDBObject().append("route", pred.getRoute()).append("lineid", pred.getLineid()).append("direction", pred.getDirid()).append("destination", pred.getDest()), new BasicDBObject().append("$setOnInsert", new BasicDBObject("day", "SD")).append("$inc", new BasicDBObject("dest_cnt", 1)), true, false, WriteConcern.UNACKNOWLEDGED);
			}
			
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

	public void performTask(String object, DBObject extra) {
		if (object.equals("withdraw")) {
			Main.mongo.update("lvf_vehicles", new BasicDBObject("uvi", this.uvi), new BasicDBObject("$unset", new BasicDBObject().append("vid", 1).append("cdreg", 1).append("whereseen", 1)));
			forceWithdraw();
		} else if (object.equals("delete")) {
			// First remove linked data
			Main.mongo.delete("lvf_history", new BasicDBObject("vid", this.uvi), true);
			
			// Now delete vehicle record
			Main.mongo.delete("lvf_vehicles", new BasicDBObject("uvi", this.uvi));
			singleton.remove(vid);
			singletonUvi.remove(uvi);
			this.exists = false;
			this.reg = "";
			this.vid = 0;
			this.uvi = 0;
		} else if (object.equals("merge")) {
			int newUvi = (Integer) extra.get("uvi");
			Bus other = getFromUvi(newUvi);
			if (other != null) {
				Main.mongo.update("lvf_vehicles", new BasicDBObject("uvi", newUvi), new BasicDBObject("$set", new BasicDBObject().append("vid", vid).append("cdreg", reg))); // Update old record
				loadHistory();
				other.mergeIn(vid, reg, history, predictions);
				
				Main.mongo.delete("lvf_vehicles", new BasicDBObject("uvi", this.uvi)); // Delete new record (us)
				Main.mongo.delete("lvf_history", new BasicDBObject("vid", this.uvi), true);
				singletonUvi.remove(uvi);
				this.exists = false;
				this.reg = "";
				this.vid = 0;
				this.uvi = 0;
			}
		}
	}

	private void mergeIn(int vid2, String reg2, Map<String, Map<String, Map<String, Map<String, Date>>>> history2, PriorityQueue<Prediction> predictions2) {
		this.exists = true;
		this.reg = reg2;
		
		singleton.remove(vid);
		singleton.put(vid2, this);
		this.vid = vid2;
		
		// Merge predictions
		while (!predictions2.isEmpty()) {
			Prediction pred = predictions2.poll();
			
			if (pred_update.containsKey(pred.getStop())) {
				predictions.remove(pred_update.get(pred.getStop()));
			}
			
			pred_update.put(pred.getStop(), pred);
			predictions.offer(pred);
		}
		updateQueue();
		
		// Merge history
		for (String date : history2.keySet()) {
			for (String route : history2.get(date).keySet()) {
				for (String lineid : history2.get(date).get(route).keySet()) {
					for (String row : history2.get(date).get(route).get(lineid).keySet()) {
						updateHistory(date, history2.get(date).get(route).get(lineid).get(row), route, lineid);
					}
				}
			}
		}
	}
	
	private void loadHistory() {
		DBCursor c = Main.mongo.find("lvf_history", new BasicDBObject("vid", this.uvi));
		while (c.hasNext()) {
			DBObject r = c.next();
			updateHistory(TFL.dateFormat.format(r.get("date")), (Date) r.get("last_seen"), (String) r.get("route"), (String) r.get("lineid"));
			updateHistory(TFL.dateFormat.format(r.get("date")), (Date) r.get("first_seen"), (String) r.get("route"), (String) r.get("lineid"));
		}
	}
	
}
