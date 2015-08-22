package uk.co.thomasc.lvf.bus;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Wrapper for a prediction
 * The persistent part of responses from TFL
 */
@AllArgsConstructor
@ToString
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
	@Getter @Setter private int visit;

	public Object[] toDbObject(int vid) {
		return new Object[] {
			vid,
			this.route,
			this.lineid,
			this.time,
			this.stop,
			this.dirid,
			this.dest,
			this.visit
		};
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		
		Prediction other = (Prediction) obj;
		return other.difftime == this.difftime && other.valid == this.valid && other.stop.equals(this.stop);
	}

}
