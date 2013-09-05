package uk.co.thomasc.lvf;

import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

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
	private Map<String, Map<String, Map<String, Map<String, Date>>>> history = new HashMap<String, Map<String, Map<String, Map<String, Date>>>>();
	private PriorityQueue<Prediction> predictions = new PriorityQueue<Prediction>(11, new Comparator<Prediction>() {
		public int compare(Prediction o1, Prediction o2) {
			return o1.getTime().before(o2.getTime()) ? -1 : 1;
		}
	});
	private Map<String, Prediction> pred_update = new HashMap<String, Prediction>();
	
	public Bus(int vid) {
		this.vid = vid;
	}
	
	public void newData(TFL tfl) {
		checkQueue();
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
			Main.mongo.update("lvf_history", new BasicDBObject().append("vid", this.vid).append("date", cal.getTime()).append("lineid", tfl.getLineid()), new BasicDBObject("$set", update), true, false, WriteConcern.UNACKNOWLEDGED);
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