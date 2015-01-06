package uk.co.thomasc.lvf;

import java.text.SimpleDateFormat;
import java.util.Date;

import lombok.Getter;
import lombok.ToString;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;

@ToString
public class TFL {

	@Getter private int type;
	@Getter private String stop;
	@Getter private int visit;
	@Getter private String lineid;
	@Getter private String route;
	@Getter private int dirid;
	@Getter private String dest;
	@Getter private int vid;
	@Getter private String reg;
	@Getter private boolean valid;
	
	@Getter private Date time;
	@Getter private String keytime;
	@Getter private String difftime;
	
	public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public static SimpleDateFormat diffDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	
	public TFL(JsonElement json) {
		JsonArray arr = (JsonArray) json;
		type = arr.get(0).getAsInt();
		if (type == 1 && !(arr.get(1) instanceof JsonNull)) {
			stop = arr.get(1).getAsString();
			visit = arr.get(2).getAsInt();
			lineid = arr.get(3).getAsString();
			route = arr.get(4).getAsString();
			dirid = arr.get(5).getAsInt();
			dest = arr.get(6).getAsString();
			vid = arr.get(7).getAsInt();
			reg = arr.get(8).getAsString();
			time = new Date(arr.get(9).getAsLong());
			valid = arr.get(10).getAsLong() != 0;
			keytime = dateFormat.format(time);
			difftime = diffDateFormat.format(time);
		} else if (type == 1) {
			type = -1;
		}
	}
	
	public Object[] toDbObject() {
		return new Object[] {vid, stop, visit, dest, route, lineid, time, dirid, valid};
	}
	
}
