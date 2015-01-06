package uk.co.thomasc.lvf.bus.destination;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class Destination {
	
	@Getter @Setter private int count;
	@Getter private String destination;
	
	public Destination(String destination) {
		this.destination = destination;
	}
	
	Destination incCount() {
		count++;
		return this;
	}
	
}
