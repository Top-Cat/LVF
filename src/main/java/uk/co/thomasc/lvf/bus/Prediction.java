package uk.co.thomasc.lvf.bus;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

@AllArgsConstructor @ToString
public class Prediction {

	@Getter @Setter private String route;
	@Getter @Setter private String lineid;
	@Getter @Setter private Date time;
	@Getter @Setter private boolean valid;
	@Getter @Setter private String keytime;
	@Getter @Setter private String difftime;
	@Getter @Setter private String stop;
	@Getter @Setter private int dirid;
	@Getter @Setter private String dest;
	
	public DBObject toDbObject() {
		return new BasicDBObject().append("route", route).append("line_id", lineid).append("last_seen", time).append("nearest_stop", stop).append("dirid", dirid).append("destination", dest);
	}
	
}