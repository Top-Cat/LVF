package uk.co.thomasc.lvf.bus;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode @AllArgsConstructor @Getter
public class HistoryKey {
	
	private String date;
	private String lineid;
	private String route;
	
}