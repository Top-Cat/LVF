package uk.co.thomasc.lvf.bus.destination;

import java.util.HashMap;

import lombok.Getter;

public class DestinationMap extends HashMap<String, Destination> {

	private static final long serialVersionUID = -563954204308575981L;
	@Getter private Destination topDest;
	@Getter private int count;
	
	public void resetCounts() {
		for (Destination dest : values()) {
			dest.setCount(0);
		}
	}

	public Destination getLargest() {
		Destination largest = null;
		
		for (String destination : keySet()) {
			Destination current = get(destination);
			if (largest == null || current.getCount() > largest.getCount()) {
				largest = current;
			}
		}
		
		return largest;
	}
	
	public void setTopDest(Destination topDest) {
		this.topDest = topDest;
		count = 1;
	}
	
	public int incCount() {
		return ++count;
	}
	
}