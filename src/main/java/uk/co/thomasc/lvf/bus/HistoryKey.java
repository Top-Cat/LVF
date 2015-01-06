package uk.co.thomasc.lvf.bus;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@AllArgsConstructor
@Getter
public class HistoryKey {

	private final String date;
	private final String lineid;
	private final String route;

}
