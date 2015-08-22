package uk.co.thomasc.lvf;

import java.text.SimpleDateFormat;
import java.util.Date;

import uk.co.thomasc.lvf.bus.Prediction;
import lombok.Getter;
import lombok.ToString;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;

/**
 * A wrapper for the data returned from TFL
 * 
 * Useful if you change the fields requested
 * as the rest of the code references the names not the indexes
 */
@ToString
public class TFL {

	@Getter private Prediction prediction;

	@Getter private int type;
	@Getter private String stop;
	@Getter private int vid;
	@Getter private String reg;
	@Getter private String stopName;
	
	public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public static SimpleDateFormat diffDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

	public TFL(JsonElement json) {
		final JsonArray arr = (JsonArray) json;
		this.type = arr.get(0).getAsInt();
		if (this.type == 1 && !(arr.get(1) instanceof JsonNull)) {
			Date time = new Date(arr.get(11).getAsLong());
			this.prediction = new Prediction(
				arr.get(6).getAsString(),
				arr.get(5).getAsString(),
				time,
				arr.get(12).getAsLong() != 0,
				dateFormat.format(time),
				diffDateFormat.format(time),
				arr.get(2).getAsString(),
				arr.get(7).getAsInt(),
				arr.get(8).getAsString(),
				arr.get(4).getAsInt()
			);
			
			this.stopName = arr.get(1).getAsString();
			this.stop = arr.get(3).getAsString();
			this.vid = arr.get(9).getAsInt();
			this.reg = arr.get(10).getAsString();
		} else if (this.type == 1) {
			this.type = -1;
		}
	}

	public Object[] toDbObject() {
		return new Object[] {
			this.prediction.getRoute(),
			this.prediction.getLineid(),
			this.prediction.getTime(),
			this.prediction.getDirid(),
			this.prediction.isValid(),
			this.vid,
			this.prediction.getStop(),
			this.prediction.getVisit(),
			this.prediction.getDest()
		};
	}

}
