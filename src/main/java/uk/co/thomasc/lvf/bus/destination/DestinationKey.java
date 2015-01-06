package uk.co.thomasc.lvf.bus.destination;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode @AllArgsConstructor @Getter
public class DestinationKey {
	
	private String route;
	private String lineid;
	private int dirid;
	
}