package uk.co.thomasc.lvf.task;

import java.util.HashMap;
import java.util.Map;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.co.thomasc.lvf.Main;

public class Tasks extends Thread {
	
	private boolean running = true;
	private boolean hasTasks = false;
	private Map<ObjectId, DBObject> tasks = new HashMap<ObjectId, DBObject>();
	
	public Tasks() {
		start();
	}
	
	@Override
	public void run() {
		while (running) {
			try {
				sleep(10000);
				
				synchronized (tasks) {
					DBCursor c = Main.mongo.find("lvf_tasks");
					while (c.hasNext()) {
						hasTasks = true;
						DBObject obj = c.next();
						tasks.put((ObjectId) obj.get("_id"), obj);
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void completed(ObjectId id) {
		synchronized (tasks) {
			tasks.remove(id);
			hasTasks = tasks.size() > 0;
			Main.mongo.delete("lvf_tasks", new BasicDBObject("_id" , id));
		}
	}
	
	public boolean hasTasks() {
		return hasTasks;
	}
	
	public void finish() {
		running = false;
	}

	public DBObject[] getTasks() {
		synchronized (tasks) {
			return tasks.values().toArray(new DBObject[0]);
		}
	}
	
}