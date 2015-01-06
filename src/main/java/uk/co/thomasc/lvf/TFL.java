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
		final JsonArray arr = (JsonArray) json;
		this.type = arr.get(0).getAsInt();
		if (this.type == 1 && !(arr.get(1) instanceof JsonNull)) {
			this.stop = arr.get(1).getAsString();
			this.visit = arr.get(2).getAsInt();
			this.lineid = arr.get(3).getAsString();
			this.route = arr.get(4).getAsString();
			this.dirid = arr.get(5).getAsInt();
			this.dest = arr.get(6).getAsString();
			this.vid = arr.get(7).getAsInt();
			this.reg = arr.get(8).getAsString();
			this.time = new Date(arr.get(9).getAsLong());
			this.valid = arr.get(10).getAsLong() != 0;
			this.keytime = dateFormat.format(this.time);
			this.difftime = diffDateFormat.format(this.time);
		} else if (this.type == 1) {
			this.type = -1;
		}
	}

	public Object[] toDbObject() {
		return new Object[] {this.vid, this.stop, this.visit, this.dest, this.route, this.lineid, this.time, this.dirid, this.valid};
	}

}
