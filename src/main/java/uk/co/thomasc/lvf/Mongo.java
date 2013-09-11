package uk.co.thomasc.lvf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;

import com.google.gson.JsonObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class Mongo {
	
	private DB buspicsDB;
	@Getter private boolean master;

	public Mongo(JsonObject login) {
		System.out.println("Connecting!");
		try {
			List<ServerAddress> seeds = new ArrayList<ServerAddress>();
			seeds.add(new ServerAddress("woking.thomasc.co.uk"));
			seeds.add(new ServerAddress("swindon.thomasc.co.uk"));
			
			MongoClient mongoClient = new MongoClient(seeds);
			mongoClient.setReadPreference(ReadPreference.nearest());
			
			buspicsDB = mongoClient.getDB(((JsonObject) login.get("mongo")).get("db").getAsString());
			buspicsDB.authenticate(((JsonObject) login.get("mongo")).get("user").getAsString(), ((JsonObject) login.get("mongo")).get("pass").getAsString().toCharArray());
			
			master = InetAddress.getByName(mongoClient.getReplicaSetStatus().getMaster().getHost()).isLoopbackAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		System.out.println("Connected");
	}
	
	public long count(String collection) {
		return buspicsDB.getCollection(collection).count();
	}

	public long count(String collection, DBObject query) {
		return buspicsDB.getCollection(collection).count(query);
	}
	
	public boolean exists(String collection, DBObject query) {
		return count(collection, query) > 0;
	}
	
	public DBObject updateAndReturn(String collection, DBObject query, DBObject update) {
		return buspicsDB.getCollection(collection).findAndModify(query, update);
	}

	public WriteResult update(String collection, DBObject query, DBObject update) {
		return update(collection, query, update, false);
	}
	
	public WriteResult update(String collection, DBObject query, DBObject update, boolean upsert) {
		return update(collection, query, update, upsert, false);
	}
	
	public WriteResult update(String collection, DBObject query, DBObject update, boolean upsert, boolean multi) {
		return update(collection, query, update, upsert, multi, WriteConcern.ACKNOWLEDGED);
	}
	
	public WriteResult update(String collection, DBObject query, DBObject update, boolean upsert, boolean multi, WriteConcern concern) {
		return buspicsDB.getCollection(collection).update(query, update, upsert, multi, concern);
	}
	
	public WriteResult delete(String collection, DBObject query, boolean multi) {
		return delete(collection, query, multi, WriteConcern.ACKNOWLEDGED);
	}
	
	public WriteResult delete(String collection, DBObject query, boolean multi, WriteConcern concern) {
		return buspicsDB.getCollection(collection).remove(query, multi, concern);
	}

	public WriteResult insert(String collection, DBObject row) {
		return insert(collection, row, WriteConcern.ACKNOWLEDGED);
	}
	
	public WriteResult insert(String collection, DBObject row, WriteConcern concern) {
		return buspicsDB.getCollection(collection).insert(row, concern);
	}
	
	public DBCursor find(String collection) {
		return buspicsDB.getCollection(collection).find();
	}
	
	public DBCursor find(String collection, DBObject query) {
		return find(collection, query, null);
	}

	public DBCursor find(String collection, DBObject query, DBObject projection) {
		return buspicsDB.getCollection(collection).find(query, projection);
	}

	public DBObject findOne(String collection, DBObject query) {
		return findOne(collection, query, null);
	}
	
	public DBObject findOne(String collection, DBObject query, DBObject projection) {
		return buspicsDB.getCollection(collection).findOne(query, projection);
	}

	public DBObject findRandom(String collection) {
		long count = count(collection);
		int id = (int) (Math.random() * count);
		
		return buspicsDB.getCollection(collection).find().skip(id).next();
	}

	public int incCounter(String counter) {
		DBObject obj = buspicsDB.getCollection("counters").findAndModify(new BasicDBObject("_id", counter), new BasicDBObject("_id", 0), null, false, new BasicDBObject("$inc", new BasicDBObject("seq", 1)), true, false);
		return ((Number) obj.get("seq")).intValue();
	}

	public void debug(String string) {
		insert("lvf_audit", new BasicDBObject().append("text", string).append("level", true));
	}

	public DBObject findAndModify(String collection, BasicDBObject query, BasicDBObject update) {
		return buspicsDB.getCollection(collection).findAndModify(query, update);
	}

}
