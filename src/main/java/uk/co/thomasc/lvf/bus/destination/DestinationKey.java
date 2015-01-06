package uk.co.thomasc.lvf.bus.destination;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@AllArgsConstructor
@Getter
public class DestinationKey {

	private final String route;
	private final String lineid;
	private final int dirid;

}
