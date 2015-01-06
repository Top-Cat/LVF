package uk.co.thomasc.lvf.bus;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

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
		return new Object[] {vid, this.route, this.lineid, this.time, this.stop, this.dirid, this.dest, this.visit};
	}

}
