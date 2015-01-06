package uk.co.thomasc.lvf.bus.destination;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class Destination {

	@Getter @Setter private int count;
	@Getter private final String destination;

	public Destination(String destination) {
		this.destination = destination;
	}

	Destination incCount() {
		this.count++;
		return this;
	}

}
